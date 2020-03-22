import hashlib
import json
import logging
import math
import multiprocessing
import os
import queue
import threading
import time
from functools import partial
from multiprocessing import Process, Value
from multiprocessing.managers import AcquirerProxy, ValueProxy
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


def BlobDownloadThread(
    chunkDecryptionQueue: multiprocessing.SimpleQueue,
    encryptedChunkQueue: queue.SimpleQueue,
    encryptedChunkQueueSize: ThreadValueLock,
    credentials: str,
    bucketName: str,
    logLevel: str,
    threadId: int
) -> None:
    # Set logging level and get logger
    setLogingLevel(logLevel)
    logger = logging.getLogger(__name__)

    # Get GCS bucket
    credentials = Credentials.from_service_account_info(json.loads(credentials))
    client = storage.Client(project=credentials.project_id, credentials=credentials)
    bucket = client.get_bucket(bucketName)

    # Create stats variables
    ticToc = TicToc()

    while True:
        # Get a task
        task: list = chunkDecryptionQueue.get()
        if task is None:
            # Only thread 0 can send None to avoid duplicates
            if threadId == 0:
                encryptedChunkQueue.put(None)
            break

        # Start timer
        ticToc.tic()

        # Extract task
        blobInfo: BlobInfo = task[0]
        path: str = task[1]
        fileSize: ValueProxy = task[2]
        fileSizeLock: AcquirerProxy = task[3]
        offset: int = task[4]
        fileSha256: bytes = task[5]

        # Download blob
        chunk = bucket.get_blob(blobInfo.name).download_as_string()

        # Stop timer
        elapsed = ticToc.toc()

        # Acquire space in queue
        encryptedChunkQueueSize.acquire(len(chunk))

        # Send blob to decryption process
        encryptedChunkQueue.put([
            blobInfo,
            path,
            fileSize,
            fileSizeLock,
            offset,
            chunk,
            elapsed,
            fileSha256
        ])

        # Print downloaded blob
        logger.info('↓ {}, size: {}, elapsed: {:.3f}s, speed: {}/s, queue: {} {}'.format(
            blobInfo.name,
            naturalsize(len(chunk)),
            elapsed,
            naturalsize(len(chunk)/elapsed),
            encryptedChunkQueue.qsize(),
            naturalsize(encryptedChunkQueueSize.getValue())
        ))


def FileWriteThread(
    decryptedChunks: list,
    decryptedChunksLock: threading.Lock,
    decryptedChunksSize: ThreadValueLock,
    lifeLock: threading.Lock,
    fileVerificationQueue: multiprocessing.SimpleQueue,
    nDecryptionWorkers: int,
    nDownloadThreads: int,
    logLevel: str
) -> None:
    # Set logging level and get logger
    setLogingLevel(logLevel)
    logger = logging.getLogger(__name__)

    # Create stats variables
    elapsedDivider = nDecryptionWorkers * nDownloadThreads
    ticToc = TicToc()
    fileElapsed = dict()

    # Keep executing as long as its 'life' was not 'acquired'
    while lifeLock.acquire(blocking=False):
        # Make sure only 1 thread is accessing decrypted chunks list
        with decryptedChunksLock:
            # Create list of processed indexes
            # If a decrypted chunk is processes, its index is put in here and then cleared after each iteration
            processedIndexes = []

            # For every decrypted chunk
            for index, decryptedChunk in enumerate(decryptedChunks):
                # Extract variables
                path: str = decryptedChunk[0]
                fileSize: ValueProxy = decryptedChunk[1]
                fileSizeLock: AcquirerProxy = decryptedChunk[2]
                offset: int = decryptedChunk[3]
                chunk: bytes = decryptedChunk[4]
                elapsed: float = decryptedChunk[5]
                fileSha256: bytes = decryptedChunk[6]

                # If file size is not at offset, skip it
                if fileSize.value != offset:
                    continue

                # Start timer
                ticToc.tic()

                # Otherwise, it is time to append chunk to file
                fp = open(path, 'ab')
                fp.write(chunk)
                fp.flush()
                fp.close()

                with fileSizeLock:
                    # Add to file size
                    fileSize.value += len(chunk)

                # Note index to be cleared
                processedIndexes.append(index)

                # Stop timer and add to file elapsed
                fileElapsed[path] = fileElapsed.get(
                    path, 0) + elapsed/elapsedDivider + ticToc.toc()
                elapsed = fileElapsed[path]

                # If it is the last chunk, then send file name and file hash to file verification processes
                if fileSha256 is not None:
                    fileVerificationQueue.put([
                        path,
                        fileSha256
                    ])

                    fileSize: int = fileSize.value
                    logger.info('← {}, size: {}, elapsed: {:.3f}s, speed: {}/s'.format(
                        path,
                        naturalsize(fileSize),
                        elapsed,
                        naturalsize(fileSize/elapsed)
                    ))

            # Clear processed chunks and release size
            processedIndexes.reverse()
            for index in processedIndexes:
                decryptedChunks.pop(index)
                decryptedChunksSize.release(len(chunk))

        # The 'while' loop acquired the lock, release it here
        lifeLock.release()

        # Pause before looping again
        time.sleep(0.1)


def ChunkDecryptionProcess(
    chunkDecryptionQueue: multiprocessing.SimpleQueue,
    credentials: str,
    bucketName: str,
    nDecryptionWorkers: int,
    nDownloadThreads: int,
    fileVerificationQueue: multiprocessing.SimpleQueue,
    decryptQueueMiB: int,
    fileWriteQueueMiB: int,
    logLevel: str,
    processId: int
) -> None:
    # Set process title
    setproctitle.setproctitle('ChunkDecryptionProcess{}'.format(
        processId
    ))

    # Set logging level and get logger
    setLogingLevel(logLevel)
    logger = logging.getLogger(__name__)

    # Create blob download threads
    encryptedChunkQueue = queue.SimpleQueue()
    encryptedChunkQueueSize = ThreadValueLock(1024*1024*decryptQueueMiB)
    blobDownloadThreads = []
    for threadId in range(nDownloadThreads):
        thread = Thread(
            target=BlobDownloadThread,
            args=[
                chunkDecryptionQueue,
                encryptedChunkQueue,
                encryptedChunkQueueSize,
                credentials,
                bucketName,
                logLevel,
                threadId
            ],
            name='BlobDownloadThread{}'.format(threadId)
        )
        blobDownloadThreads.append(thread)

    # Create file write thread
    decryptedChunks = []
    decryptedChunksLock = threading.Lock()
    decryptedChunksSize = ThreadValueLock(1024*1024*fileWriteQueueMiB)
    fileWriteThreadLifeLock = threading.Lock()
    fileWriteThread = Thread(
        target=FileWriteThread,
        args=[
            decryptedChunks,
            decryptedChunksLock,
            decryptedChunksSize,
            fileWriteThreadLifeLock,
            fileVerificationQueue,
            nDecryptionWorkers,
            nDownloadThreads,
            logLevel
        ],
        name='FileWriteThread'
    )

    # Start threads
    for thread in blobDownloadThreads:
        thread.start()
    fileWriteThread.start()

    # Create stats variables
    ticToc = TicToc()

    # Start processing
    while True:
        # Get task
        task = encryptedChunkQueue.get()
        if task is None:
            break

        # Start timer
        ticToc.tic()

        # Extract task
        blobInfo: BlobInfo = task[0]
        path: str = task[1]
        fileSize: ValueProxy = task[2]
        fileSizeLock: AcquirerProxy = task[3]
        offset: int = task[4]
        chunk: bytes = task[5]
        elapsed: float = task[6]
        fileSha256: bytes = task[7]

        # Update process title
        setproctitle.setproctitle('ChunkDecryptionProcess{} {}, offset: {}'.format(
            processId,
            path,
            naturalsize(offset)
        ))

        # Release size
        encryptedChunkQueueSize.release(len(chunk))

        # Decrypt chunk
        assert blobInfo.encryptedSize == len(chunk)
        assert blobInfo.encryptedSha256 == hashlib.sha256(chunk).digest()
        chunk = Fernet(blobInfo.encryptionKey).decrypt(chunk)
        assert blobInfo.decryptedSize == len(chunk)
        assert blobInfo.decryptedSha256 == hashlib.sha256(chunk).digest()

        # Stop timer
        elapsed += ticToc.toc()

        # Send to file write thread
        decryptedChunksSize.acquire(len(chunk))
        with decryptedChunksLock:
            decryptedChunks.append([
                path,
                fileSize,
                fileSizeLock,
                offset,
                chunk,
                elapsed,
                fileSha256
            ])

        # Print decrypted chunk
        logger.info('√ {}, size: {}, elapsed: {:.3f}s, speed: {}/s, list: {} {}'.format(
            blobInfo.name,
            naturalsize(len(chunk)),
            elapsed,
            naturalsize(len(chunk)/elapsed),
            len(decryptedChunks),
            naturalsize(decryptedChunksSize.getValue())
        ))

        # Reset process title
        setproctitle.setproctitle('ChunkDecryptionProcess{}'.format(
            processId
        ))

    # Stop threads
    for thread in blobDownloadThreads:
        thread.join()
    fileWriteThreadLifeLock.acquire()
    fileWriteThread.join()


def FileVerificationProcess(
    fileVerificationQueue: multiprocessing.SimpleQueue,
    fileCompleteQueue: multiprocessing.SimpleQueue,
    logLevel: str,
    processId: int
) -> None:
    setproctitle.setproctitle('FileVerificationProcess{}'.format(
        processId
    ))

    # Set log level and get logger
    setLogingLevel(logLevel)
    logger = logging.getLogger(__name__)

    while True:
        # Get task
        task = fileVerificationQueue.get()
        if task is None:
            break

        # Extract task
        path: str = task[0]
        fileSha256: bytes = task[1]

        # Get total chunks
        fileSize = os.stat(path).st_size
        nChunks = math.ceil(fileSize / (1024*1024*32))

        # Hash the file
        sha256 = hashlib.sha256()
        with open(path, 'rb') as fp:
            for index, chunk in enumerate(iter(lambda: fp.read(1024*1024*32), bytes(0))):
                setproctitle.setproctitle('FileVerificationProcess{} {}, chunk {}/{}'.format(
                    processId,
                    path,
                    index,
                    nChunks
                ))
                sha256.update(chunk)

        # Sha256 must match
        assert sha256.digest() == fileSha256

        # Send file to complete queue
        fileCompleteQueue.put(path)

        # Print info
        logger.info('√ {}'.format(path))


def restoreCommand(
    databaseFileName: str,
    prefix: str,
    logLevel: str,
    swapPrefix: list = ['/', '/'],
    nDecryptionWorkers: int = 3,
    nDownloadThreads: int = 2,
    decryptQueueMiB: int = 256,
    fileWriteQueueMiB: int = 256,
    nFileVerificationWorkers: int = 1
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

    # Set process title
    setproctitle.setproctitle('RestoreCommand')

    # Set logging level and get logger
    setLogingLevel(logLevel)
    logger = logging.getLogger(__name__)

    # If total number of workers exceeds cpu count, print warning
    if nDecryptionWorkers+nFileVerificationWorkers > multiprocessing.cpu_count():
        logger.warning('Total number of workers exceeds cpu count')

    # Print parameters
    logger.info('database: {}, prefix: {}, swapPrefix: {}, nDecryptionWorkers: {}, nDownloadThreads: {}, decryptQueueMiB: {}, fileWriteQueueMiB: {}, nFileVerificationWorkers: {}'.format(
        databaseFileName,
        prefix,
        swapPrefix,
        nDecryptionWorkers,
        nDownloadThreads,
        decryptQueueMiB,
        fileWriteQueueMiB,
        nFileVerificationWorkers
    ))

    # Create queues
    chunkDecryptionQueue = multiprocessing.SimpleQueue()
    fileVerificationQueue = multiprocessing.SimpleQueue()
    fileCompleteQueue = multiprocessing.SimpleQueue()

    # Get transient database
    database = Database.getTransientCopy(databaseFileName)

    # Get credentials file and bucket name
    result = getGcsParameters(databaseFileName)
    credentials: str = result[0]
    bucketName: str = result[1]

    # Create chunk decryption processes
    chunkDecryptionProcesses = []
    for processId in range(nDecryptionWorkers):
        process = Process(
            target=ChunkDecryptionProcess,
            args=[
                chunkDecryptionQueue,
                credentials,
                bucketName,
                nDecryptionWorkers,
                nDownloadThreads,
                fileVerificationQueue,
                decryptQueueMiB,
                fileWriteQueueMiB,
                logLevel,
                processId
            ],
            name='ChunkDecryptionProcess{}'.format(processId)
        )
        chunkDecryptionProcesses.append(process)

    # Create file hashing processes
    fileVerificationProcesses = []
    for processId in range(nFileVerificationWorkers):
        process = Process(
            target=FileVerificationProcess,
            args=[
                fileVerificationQueue,
                fileCompleteQueue,
                logLevel,
                processId
            ],
            name='FileVerificationProcess{}'.format(processId)
        )
        fileVerificationProcesses.append(process)

    # Start processes
    for process in chunkDecryptionProcesses:
        process.start()
    for process in fileVerificationProcesses:
        process.start()

    # Get sync manager
    syncManager = multiprocessing.Manager()

    # Convert to absolute path
    prefix = os.path.abspath(prefix)
    swapPrefix[0] = os.path.abspath(swapPrefix[0])
    swapPrefix[1] = os.path.abspath(swapPrefix[1])

    # You have to always to keep a reference to whatever SyncManager gives to you
    # Otherwise it throws multiprocessing.managers.RemoteError
    # Here I use list to keep the reference
    fileSizes = []
    fileSizeLocks = []

    # Generate decryption tasks
    paths = []
    for path in database.selectPaths(os.path.abspath(prefix)):
        fileInfo = database.getFile(path)
        fileSize = syncManager.Value('Q', 0, lock=False)    # 'Q' == unsigned long long
        fileSizeLock = syncManager.Lock()   # pylint: disable=E1101
        offset = 0

        # Keep the reference
        fileSizes.append(fileSize)
        fileSizeLocks.append(fileSizeLock)

        # Warn user if file does not start with prefix
        if not path.startswith(swapPrefix[0]):
            logger.warning('Unable to swap prefix for file {}, it will be ignored.'.format(
                path
            ))
            continue

        # Swap prefix
        path = path.replace(swapPrefix[0], swapPrefix[1], 1)

        # Create folders leading to the file
        os.makedirs(os.path.dirname(path), exist_ok=True)

        # Create or clear file
        fp = open(path, 'wb')
        fp.close()

        # Create task for each chunk in file
        for index, blobId in enumerate(fileInfo.blobIds):
            blobInfo = database.getBlob(blobId)
            fileSha256 = fileInfo.sha256 if index == len(
                fileInfo.blobIds)-1 else None

            chunkDecryptionQueue.put([
                blobInfo,
                path,
                fileSize,
                fileSizeLock,
                offset,
                fileSha256
            ])

            offset += blobInfo.decryptedSize

        # Add file to list
        paths.append(path)

    # While waiting for all files to be processed,
    # Set file mode, uid, gid, atime, mtime
    nPathsProcessed = 0
    while nPathsProcessed < len(paths):
        # Get path
        path: str = fileCompleteQueue.get()
        # Restore original path
        originalPath = path.replace(swapPrefix[1], swapPrefix[0], 1)

        # Retrieve file info from database
        fileInfo: FileInfo = database.getFile(originalPath)
        stats = fileInfo.stats

        # Restore mode
        os.chmod(path, stats['mode'])

        # Restore uid and gid, ignore error
        try:
            os.chown(path, stats['uid'], stats['gid'])
        except PermissionError:
            pass

        # Restore atime and mtime
        os.utime(path, (stats['atime'], stats['mtime']))

        nPathsProcessed += 1

    # Close database
    database.close()

    # Wait for processes to exit
    for _ in range(nDecryptionWorkers * nDownloadThreads):
        chunkDecryptionQueue.put(None)
    for process in chunkDecryptionProcesses:
        process.join()

    for _ in range(nFileVerificationWorkers):
        fileVerificationQueue.put(None)
    for process in fileVerificationProcesses:
        process.join()


if __name__ == '__main__':
    print('The entry point of this program is in commandline.py')
    print('Use command \'python3 commandline.py -h\'')
