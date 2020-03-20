import hashlib
import json
import logging
import math
import multiprocessing
import os
import queue
import random
import threading
from functools import partial
from multiprocessing import Pool, Process
from threading import Thread

import setproctitle
from cryptography.fernet import Fernet
from google.cloud import storage
from google.oauth2.service_account import Credentials
from humanize import naturalsize
from ttictoc import TicToc

from database import BlobInfo, Database, FileInfo
from valuelock import ThreadValueLock

naturalsize = partial(naturalsize, binary=True)


def setLogingLevel(
    logLevel: str
) -> None:
    numericLevel = getattr(logging, logLevel.upper(), None)
    if isinstance(numericLevel, int):
        logging.basicConfig(level=numericLevel)


def BlobUploadThread(
    blobUploadQueue: queue.SimpleQueue,
    blobUploadQueueBytes: ThreadValueLock,
    credentials: str,
    bucketName: str,
    threadId: int
) -> None:
    # Get GCS bucket
    credentials = Credentials.from_service_account_info(json.loads(credentials))
    client = storage.Client(project=credentials.project_id, credentials=credentials)
    bucket = client.get_bucket(bucketName)

    # Process tasks until received None
    while True:
        # Get task
        task = blobUploadQueue.get()
        if task is None:
            break

        # Extract task
        name: str = task[0]
        blob: bytes = task[1]

        # Release available space counter
        blobUploadQueueBytes.release(len(blob))

        # Upload blob
        bucket.blob(name).upload_from_string(blob)


def ChunkEncryptionProcess(
    chunkEncryptionQueue: multiprocessing.SimpleQueue,
    blobInfoQueue: multiprocessing.SimpleQueue,
    credentials: str,
    bucketName: str,
    processId: int,
    nUploadThreads: int,
    uploadQueueMiB: int
) -> None:
    def readChunk(
        path: str,
        offset: int,
        length: int = 1024*1024*32
    ) -> bytes:
        fp = open(path, 'rb')
        assert offset == fp.seek(offset)
        chunk = fp.read(length)
        fp.close()
        return chunk

    def encryptChunk(
        chunk: bytes
    ) -> list:
        # Choose a random name
        name = ''.join(random.choice('0123456789abcdef') for i in range(32))

        # Get decrypted stats
        decryptedSize = len(chunk)
        decryptedSha256 = hashlib.sha256(chunk).digest()

        # Generate key and encrypt chunk
        # Discard decrypted chunk at the same time
        encryptionKey = Fernet.generate_key()
        chunk = Fernet(encryptionKey).encrypt(chunk)

        # Get encrypted stats
        encryptedSize = len(chunk)
        encryptedSha256 = hashlib.sha256(chunk).digest()

        # Generate blob info
        blobInfo = BlobInfo(
            name,
            encryptionKey,
            encryptedSize,
            encryptedSha256,
            decryptedSize,
            decryptedSha256
        )

        return [chunk, blobInfo]

    # Start blob upload threads
    blobUploadQueueBytes = ThreadValueLock(1024*1024*uploadQueueMiB)
    blobUploadQueue = queue.SimpleQueue()
    blobUploadThreads = []
    for threadId in range(nUploadThreads):
        thread = Thread(
            target=BlobUploadThread,
            args=[
                blobUploadQueue,
                blobUploadQueueBytes,
                credentials,
                bucketName,
                threadId
            ],
            name='BlobUploadThread{}'.format(threadId)
        )
        thread.start()
        blobUploadThreads.append(thread)

    # Create stats variable
    ticToc = TicToc()

    # Process tasks until received None
    while True:
        # Set process title
        setproctitle.setproctitle('ChunkEncryptionProcess{}'.format(
            processId
        ))

        # Get task
        task = chunkEncryptionQueue.get()
        if task is None:
            break

        # Start measuring time used for encryption
        elapsed = []
        ticToc.tic()

        # Extract task
        path: str = task[0]
        offset: int = task[1]
        fileSize: int = task[2]

        # Update process title
        setproctitle.setproctitle('ChunkEncryptionProcess{} {}, chunk {}/{}'.format(
            processId,
            path,
            offset // (1024*1024*32),
            math.ceil(fileSize / (1024*1024*32))
        ))

        # Read chunk
        chunk = readChunk(path, offset)

        # Encrypt chunk
        # Discard decrypted chunk at the same time
        result: list = encryptChunk(chunk)
        chunk: bytes = result[0]
        blobInfo: BlobInfo = result[1]

        # Stop measuring time used for encryption
        elapsed.append(ticToc.toc())

        # Start measuring time used waiting for upload queue
        ticToc.tic()

        # Send encrypted chunk to blob upload threads
        blobUploadQueueBytes.acquire(len(chunk))
        blobUploadQueue.put([blobInfo.name, chunk])

        # Stop measuring time used waiting for upload queue
        elapsed.append(ticToc.toc())

        # Send blob info to blob info collection process
        blobInfoQueue.put([
            path,
            offset,
            blobInfo,
            elapsed,
            blobUploadQueue.qsize(),
            blobUploadQueueBytes.getValue()
        ])

    # Stop blob upload threads
    for _ in range(nUploadThreads):
        blobUploadQueue.put(None)
    for thread in blobUploadThreads:
        thread.join()


def BlobRemoveProcess(
    blobRemoveQueue: multiprocessing.SimpleQueue,
    credentials: str,
    bucketName: str
) -> None:
    # Get GCS bucket
    credentials = Credentials.from_service_account_info(json.loads(credentials))
    client = storage.Client(project=credentials.project_id, credentials=credentials)
    bucket = client.get_bucket(bucketName)

    # Process tasks until received None
    while True:
        # Set process title
        setproctitle.setproctitle('BlobRemoveProcess')

        # Get task
        task = blobRemoveQueue.get()
        if task is None:
            break

        # Extract task
        name: str = task[0]

        # Update process title
        setproctitle.setproctitle('BlobRemoveProcess {}'.format(name))

        # Remove blob
        try:
            bucket.delete_blob(name)
        except:
            print('Exception while deleting blob {}'.format(name))


def EncryptionTaskGenerationProcess(
    chunkEncryptionQueue: multiprocessing.SimpleQueue,
    blobRemoveQueue: multiprocessing.SimpleQueue,
    databaseUpdateQueue: multiprocessing.SimpleQueue,
    databaseFileName: str,
    path: str
) -> None:
    def isFileChanged(
        database: Database,
        path: str
    ) -> bool:
        # File is "changed" if it is not in database
        fileInfo = database.getFile(path)
        if fileInfo is None:
            return True

        # Consider file is not changed if modification time is the same
        stat = os.stat(path)
        if fileInfo.stats['mtime'] == stat.st_mtime:
            return False

        # File is changed if file size is different
        if fileInfo.decryptedSize != stat.st_size:
            return True

        # Check actual content of the file by read in chunk by chunk
        fp = open(path, 'rb')
        for index, chunk in enumerate(iter(lambda: fp.read(1024*1024*32), bytes(0))):
            blobInfo = database.getBlob(fileInfo.blobIds[index])
            assert blobInfo is not None

            # File is changed if chunk hash is different
            if blobInfo.decryptedSha256 != hashlib.sha256(chunk).digest():
                return True

        # Otherwise the file is definitely not changed
        return False

    # Set process title
    setproctitle.setproctitle('EncryptionTaskGenerationProcess')

    # Get a transient database
    database = Database.getTransientCopy(databaseFileName)

    # Convert to absolute path
    path = os.path.abspath(path)

    # Walk over all files under path
    # Use path itself if it is a file
    for dirPath, _, fileNames in [('', None, [path])] if os.path.isfile(path) else os.walk(path):
        # Update process title
        setproctitle.setproctitle('EncryptionTaskGenerationProcess {}, {} files'.format(
            dirPath,
            len(fileNames)
        ))

        fileNames.sort()

        for fileName in fileNames:
            filePath = os.path.join(dirPath, fileName)

            # If file is not changed, then only update file modification time
            if not isFileChanged(database, filePath):
                # If mtime is different, send update mtime command to queue
                if database.getFile(filePath).stats['mtime'] != os.stat(filePath).st_mtime:
                    databaseUpdateQueue.put(['UpdateMtime', filePath])
                continue

            # Otherwise backup the file
            # If file is present in database, remove existing blobs
            fileInfo = database.getFile(filePath)
            if fileInfo is not None:
                for blobId in fileInfo.blobIds:
                    # Send blob name to blob remove process
                    blobInfo = database.getBlob(blobId)
                    blobRemoveQueue.put([blobInfo.name])

                    # Send remove blob command to queue
                    databaseUpdateQueue.put(['RemoveBlob', blobId])

            # Send chunk info to chunk encryption process
            fileSize = os.stat(filePath).st_size
            for offset in range(0, fileSize, 1024*1024*32):
                chunkEncryptionQueue.put([filePath, offset, fileSize])

    # Close transient database
    database.close()


def FileHashingPoolFunction(
    path: str
) -> bytes:
    # Set process title
    setproctitle.setproctitle('FileHashingPoolFunction')

    # Get total chunks
    fileSize = os.stat(path).st_size
    nChunks = math.ceil(fileSize / (1024*1024*32))

    # Hash the file
    sha256 = hashlib.sha256()
    with open(path, 'rb') as fp:
        for index, chunk in enumerate(iter(lambda: fp.read(1024*1024*32), bytes(0))):
            setproctitle.setproctitle('FileHashingPoolFunction {}, chunk {}/{}'.format(
                path,
                index,
                nChunks
            ))
            sha256.update(chunk)

    # Reset process title
    setproctitle.setproctitle('FileHashingPoolFunction')

    # Return hash
    return sha256.digest()


def BlobInfoCollectionProcess(
    blobInfoQueue: multiprocessing.SimpleQueue,
    databaseFileName: str,
    logLevel: str,
    nEncryptionWorkers: int,
    nFileHashingWorkers: int
) -> None:
    def sendToDatabase(
        database: Database,
        logger: logging.Logger,
        path: str,
        record: list,
        nEncryptionWorkers: int
    ) -> int:
        # Sort blob info according to offset
        blobInfoPairs: list = record[2]
        blobInfoPairs.sort(key=lambda blobInfoPair: blobInfoPair[0])
        blobInfos = [blobInfoPair[1] for blobInfoPair in blobInfoPairs]

        # Create stats variables
        decryptedSize = 0
        encryptedSize = 0

        # Insert blob info into database
        blobIds = []
        for blobInfo in blobInfos:
            blobId = database.setBlob(blobInfo)
            blobIds.append(blobId)

            decryptedSize += blobInfo.decryptedSize
            encryptedSize += blobInfo.encryptedSize
        assert decryptedSize == record[0]

        # Wait and get hash of file
        filehashingResult: multiprocessing.pool.AsyncResult = record[4]
        filehashingResult.wait()
        sha256 = filehashingResult.get()

        # Get stat of file
        osStat = os.stat(path)
        stats = {
            'mode': osStat.st_mode,
            'uid': osStat.st_uid,
            'gid': osStat.st_gid,
            'atime': osStat.st_atime,
            'mtime': osStat.st_mtime,
        }

        # Insert file info into database
        fileInfo = FileInfo(
            path,
            sha256,
            stats,
            encryptedSize,
            decryptedSize,
            blobIds
        )
        database.setFile(fileInfo)

        # Print info
        elapsed = record[3]
        logger.info('+ {}, before: {}, after: {}, blobs: {}, elapsed: {:.3f}s, speed: {}/s {}/s'.format(
            path,
            naturalsize(decryptedSize),
            naturalsize(encryptedSize),
            len(blobIds),
            elapsed,
            naturalsize(decryptedSize/elapsed*nEncryptionWorkers),
            naturalsize(encryptedSize/elapsed*nEncryptionWorkers)
        ))

        return encryptedSize

    # Set process title
    setproctitle.setproctitle('BlobInfoCollectionProcess')

    # Set logging level and get logger
    setLogingLevel(logLevel)
    logger = logging.getLogger(__name__)

    # Open database
    database = Database(databaseFileName)

    # Create record pool
    recordPool = dict()

    # Create file hashing pool
    fileHashingPool = Pool(processes=nFileHashingWorkers)

    # Create stats variables
    decryptedSize = 0
    encryptedSize = 0
    ticToc = TicToc()

    # Process tasks until received None
    ticToc.tic()
    while True:
        # Get task
        task = blobInfoQueue.get()
        if task is None:
            break

        # Extract task
        path: str = task[0]
        offset: int = task[1]
        blobInfo: BlobInfo = task[2]
        elapsed: list = task[3]
        uploadQueueSize: int = task[4]
        uploadQueueBytes: int = task[5]

        # Print info
        logger.info('+ {}, before: {}, after: {}, elapsed: {:.3f}s {:.3f}s, speed: {}/s {}/s, queue: {} {}'.format(
            blobInfo.name,
            naturalsize(blobInfo.decryptedSize),
            naturalsize(blobInfo.encryptedSize),
            elapsed[0], elapsed[1],
            naturalsize(blobInfo.decryptedSize/elapsed[0]),
            naturalsize(blobInfo.encryptedSize/elapsed[0]),
            uploadQueueSize,
            naturalsize(uploadQueueBytes)
        ))

        # Get file record from pool
        if path in recordPool:
            record = recordPool[path]
        # Create a new record if there is no record
        else:
            # Start hashing the file
            filehashingResult = fileHashingPool.apply_async(
                func=FileHashingPoolFunction,
                args=[path]
            )

            # Create new record
            record = [
                os.stat(path).st_size,      # File size
                0,                          # Total bytes processed
                [],                         # BlobInfo[]
                0,                          # Total elapsed time processing the file
                filehashingResult           # Async hashing result
            ]

            # Insert new record
            recordPool[path] = record

        # Update record
        record[1] += blobInfo.decryptedSize
        record[2].append([offset, blobInfo])
        record[3] += elapsed[0]+elapsed[1]

        # If file is completely processed, send file record to database
        if record[0] == record[1]:
            # Update process title
            setproctitle.setproctitle('BlobInfoCollectionProcess {}'.format(
                path
            ))

            decryptedSize += record[0]
            encryptedSize += sendToDatabase(
                database,
                logger,
                path,
                record,
                nEncryptionWorkers
            )

            # Remove file record from pool
            recordPool.pop(path)

            # Reset process title
            setproctitle.setproctitle('BlobInfoCollectionProcess')

    # Stop timer
    elapsed = ticToc.toc()

    # Close file hashing pool
    fileHashingPool.close()
    fileHashingPool.join()

    # Close database
    database.commit()
    database.close()

    # Print info
    logger.info('elapsed: {:.3f}s, avg speed: {}/s {}/s'.format(
        elapsed,
        naturalsize(decryptedSize/elapsed),
        naturalsize(encryptedSize/elapsed)
    ))


def backupCommand(
    databaseFileName: str,
    path: str,
    logLevel: str,
    nEncryptionWorkers: int = 3,
    nUploadThreads: int = 2,
    uploadQueueMiB: int = 256,
    nFileHashingWorkers: int = 1
) -> None:
    def getGcsParameters(
        databaseFileName: str
    ) -> list:
        # Get credentials and bucket name from database
        database = Database.getTransientCopy(databaseFileName)
        credentials = database.getPair('credentials')
        bucketName = database.getPair('bucketName')
        database.close()

        return [credentials, bucketName]

    def processDatabaseUpdate(
        databaseFileName: str,
        databaseUpdateQueue: multiprocessing.SimpleQueue
    ) -> None:
        # Open database
        database = Database(databaseFileName)

        # Process updates
        while not databaseUpdateQueue.empty():
            # Get a task
            task = databaseUpdateQueue.get()
            taskName = task[0]

            # Update modification time
            if taskName == 'UpdateMtime':
                filePath = task[1]

                # Get file info
                fileInfo = database.getFile(filePath)
                # Update mtime
                fileInfo.stats['mtime'] = os.stat(filePath).st_mtime
                # Update database record
                database.setFile(fileInfo)

            # Remove a blob
            if taskName == 'RemoveBlob':
                blobId = task[1]

                # Remove blob
                database.removeBlob(blobId)

        # Close database
        database.commit()
        database.close()

    # Set process title
    setproctitle.setproctitle('BackupCommand')

    # Set logging level and get logger
    setLogingLevel(logLevel)
    logger = logging.getLogger(__name__)

    # If total number of workers exceeds cpu count, print warning
    if nEncryptionWorkers+nFileHashingWorkers > multiprocessing.cpu_count():
        logger.warning('Total number of workers exceeds cpu count')

    # Print parameters
    logger.info('database: {}, path: {}, nEncryptionWorkers: {}, nUploadThreads: {}, uploadQueueMiB: {}, nFileHashingWorkers: {}'.format(
        databaseFileName,
        path,
        nEncryptionWorkers,
        nUploadThreads,
        uploadQueueMiB,
        nFileHashingWorkers
    ))

    # Create queues
    blobInfoQueue = multiprocessing.SimpleQueue()
    chunkEncryptionQueue = multiprocessing.SimpleQueue()
    blobRemoveQueue = multiprocessing.SimpleQueue()
    databaseUpdateQueue = multiprocessing.SimpleQueue()

    # Get credentials file and bucket name
    result = getGcsParameters(databaseFileName)
    credentials: str = result[0]
    bucketName: str = result[1]

    # Create encryption task generation process
    encryptionTaskGenerationProcess = Process(
        target=EncryptionTaskGenerationProcess,
        args=[
            chunkEncryptionQueue,
            blobRemoveQueue,
            databaseUpdateQueue,
            databaseFileName,
            path
        ],
        name='EncryptionTaskGenerationProcess'
    )

    # Create blob remove process
    blobRemoveProcess = Process(
        target=BlobRemoveProcess,
        args=[
            blobRemoveQueue,
            credentials,
            bucketName
        ],
        name='BlobRemoveProcess'
    )

    # Create chunk encryption processes
    chunkEncryptionProcesses = []
    for processId in range(nEncryptionWorkers):
        process = Process(
            target=ChunkEncryptionProcess,
            args=[
                chunkEncryptionQueue,
                blobInfoQueue,
                credentials,
                bucketName,
                processId,
                nUploadThreads,
                uploadQueueMiB
            ],
            name='ChunkEncryptionProcess{}'.format(processId)
        )
        chunkEncryptionProcesses.append(process)

    # Create blob info collection process
    blobInfoCollectionProcess = Process(
        target=BlobInfoCollectionProcess,
        args=[
            blobInfoQueue,
            databaseFileName,
            logLevel,
            nEncryptionWorkers,
            nFileHashingWorkers
        ],
        name='BlobInfoCollectionProcess'
    )

    # Start processes
    encryptionTaskGenerationProcess.start()
    blobRemoveProcess.start()
    for process in chunkEncryptionProcesses:
        process.start()
    blobInfoCollectionProcess.start()

    # Wait for all files in path has been assigned
    encryptionTaskGenerationProcess.join()

    # Wait for blob remove process to exit
    blobRemoveQueue.put(None)
    blobRemoveProcess.join()

    # Wait for chunk encryption process to exit
    for _ in range(nEncryptionWorkers):
        chunkEncryptionQueue.put(None)
    for process in chunkEncryptionProcesses:
        process.join()

    # Wait for blob info collection process to exit
    blobInfoQueue.put(None)
    blobInfoCollectionProcess.join()

    # Process updates to database
    processDatabaseUpdate(
        databaseFileName,
        databaseUpdateQueue
    )


if __name__ == '__main__':
    print('The entry point of this program is in commandline.py')
    print('Use command \'python3 commandline.py -h\'')
