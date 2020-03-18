import hashlib
import os
import random
from multiprocessing import Pool, Process, Queue
from tempfile import NamedTemporaryFile

from cryptography.fernet import Fernet
from google.cloud import storage

from database import BlobInfo, Database, FileInfo


class BackupCommand:
    def __init__(self, database: Database, nEncryptionWorkers: int, nUploadWorkers: int):
        self.database = database
        self.nEncryptionWorkers = nEncryptionWorkers
        self.nUploadWorkers = nUploadWorkers

    def backupPath(self, path: str) -> None:
        # Read credentials and bucket name from database
        bucketName = self.database.getPair('bucketName')
        credentials = self.database.getPair('credentials').encode()

        # Start encryption workers using Pool
        encryptionWorkerPool = Pool(processes=self.nEncryptionWorkers)
        for processId, process in enumerate(encryptionWorkerPool._pool):
            process.name = 'EncryptionWorker[{}]'.format(processId)

        # Start upload workers using Process list
        uploadTaskQueue = Queue(self.nUploadWorkers * 2)
        uploadWorkers = []
        for processId in range(self.nUploadWorkers):
            worker = Process(
                target=BackupCommand.uploadWorker,
                args=[credentials, bucketName, uploadTaskQueue],
                name='UploadWorker[{}]'.format(processId)
            )
            worker.start()
            uploadWorkers.append(worker)

        # Start remove workers using Process list
        removeTaskQueue = Queue(self.nUploadWorkers * 2)
        removeWorkers = []
        for processId in range(self.nUploadWorkers):
            worker = Process(
                target=BackupCommand.removeWorker,
                args=[credentials, bucketName, removeTaskQueue],
                name='RemoveWorker[{}]'.format(processId)
            )
            worker.start()
            removeWorkers.append(worker)

        # Convert to absolute path
        path = os.path.abspath(path)

        # Walk over all files under path
        # If path is a file, this step does nothing
        for dirPath, _, fileNames in os.walk(path):
            for fileName in fileNames:
                filePath = os.path.join(dirPath, fileName)

                # If file is not changed, then only update file modification time
                if not self.isFileChanged(filePath):
                    fileInfo = self.database.getFile(filePath)
                    fileInfo.stats['mtime'] = os.stat(filePath).st_mtime
                    self.database.setFile(fileInfo)
                # Otherwise backup the file
                else:
                    # If present, remove existing blobs
                    fileInfo = self.database.getFile(filePath)
                    if fileInfo is not None:
                        for blobId in fileInfo.blobIds:
                            blobName = self.database.getBlob(blobId).name
                            self.database.removeBlob(blobId)
                            removeTaskQueue.put(blobName)

                    # Backup the file
                    self.backupFile(filePath, encryptionWorkerPool, uploadTaskQueue)

        # Remember to process it if path itself is a file
        if os.path.isfile(path):
            self.backupFile(path, encryptionWorkerPool, uploadTaskQueue)

        # Close encryption workers
        encryptionWorkerPool.close()
        encryptionWorkerPool.join()

        # Close upload workers
        for _ in range(len(uploadWorkers)):
            uploadTaskQueue.put(None)
        for worker in uploadWorkers:
            worker.join()

        # Close remove workers
        for _ in range(len(removeWorkers)):
            removeTaskQueue.put(None)
        for worker in removeWorkers:
            worker.join()

    def backupFile(self, path: str, encryptionWorkerPool: Pool, uploadTaskQueue: Queue):
        # Initialize variables
        sha256 = hashlib.sha256()
        blobIds = []
        encryptedSize = 0
        decryptedSize = 0

        # Read the whole file
        fp = open(path, 'rb')
        while not fp.closed:
            chunks = []

            # Read up to (number of processes) chunks at a time
            for _ in range(len(encryptionWorkerPool._pool)):
                chunk = fp.read(1024*1024*32)

                # If last chunk is empty, then it is at EOF
                if len(chunk) == 0:
                    fp.close()
                    break

                chunks.append(chunk)

            # Fire encryption tasks
            # It may take a while for workers to finish
            asyncResult = encryptionWorkerPool.map_async(
                func=BackupCommand.encryptionWorker,
                iterable=chunks
            )

            # While workers are working, compute file hash
            for chunk in chunks:
                sha256.update(chunk)
            # No need to keep holding the chunks, release them?
            chunks = []

            # Wait until workers to finish
            asyncResult.wait()

            # Collect encryption results
            for result in asyncResult.get():
                name: str = result[0]
                encryptedChunk: bytes = result[1]
                blobInfo: BlobInfo = result[2]

                encryptedSize += blobInfo.encryptedSize
                decryptedSize += blobInfo.decryptedSize

                # Send encrypted chunks to upload queue
                uploadTaskQueue.put([name, encryptedChunk])

                # Record blob in database and take note of blob ID
                blobIds.append(self.database.setBlob(blobInfo))

        # Record system stats of file, will be used when restoring the file in the future
        osStat = os.stat(path)
        stats = {
            'mode': osStat.st_mode,
            'uid': osStat.st_uid,
            'gid': osStat.st_gid,
            'atime': osStat.st_atime,
            'mtime': osStat.st_mtime,
        }

        # Record file in database
        self.database.setFile(FileInfo(path, sha256.digest(), stats, encryptedSize, decryptedSize, blobIds))

    def isFileChanged(self, path) -> bool:
        # File is "changed" is there is not in database
        fileInfo = self.database.getFile(path)
        if fileInfo is None:
            return True

        # File is not changed if modification time is the same
        stat = os.stat(path)
        if fileInfo.stats['mtime'] == stat.st_mtime:
            return False

        # File is changed if file size is different
        if fileInfo.decryptedSize != stat.st_size:
            return True

        # Check actual content of the file by read in chunk by chunk
        fp = open(path, 'rb')
        for index, chunk in enumerate(iter(lambda: fp.read(1024*1024*32), bytes(0))):
            blobInfo = self.database.getBlob(fileInfo.blobIds[index])
            assert blobInfo is not None

            # File is changed if chunk hash is different
            if blobInfo.decryptedSha256 != hashlib.sha256(chunk).digest():
                return True

        # Otherwise the file is definitely not changed
        return False

    @staticmethod
    def encryptionWorker(chunk: bytes) -> list:
        name = ''.join(random.choice('0123456789abcdef') for i in range(32))

        decryptedSize = len(chunk)
        decryptedSha256 = hashlib.sha256(chunk).digest()

        encryptionKey = Fernet.generate_key()
        chunk = Fernet(encryptionKey).encrypt(chunk)

        encryptedSize = len(chunk)
        encryptedSha256 = hashlib.sha256(chunk).digest()

        blobInfo = BlobInfo(name, encryptionKey, encryptedSize,encryptedSha256, decryptedSize, decryptedSha256)
        return [name, chunk, blobInfo]

    @staticmethod
    def uploadWorker(credentials: bytes, bucketName: str, taskQueue: Queue):
        # Create temporary JSON file, get client, and get bucket
        credentialsFile = NamedTemporaryFile(mode='wb', buffering=0, suffix='.json')
        credentialsFile.write(credentials)
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentialsFile.name
        client = storage.Client()
        bucket = client.get_bucket(bucketName)
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = ''
        credentialsFile.close()

        while True:
            task = taskQueue.get()
            if task is None:
                return

            name: str = task[0]
            data: bytes = task[1]

            bucket.blob(name).upload_from_string(data)

    @staticmethod
    def removeWorker(credentials: bytes, bucketName: str, taskQueue: Queue):
        # Create temporary JSON file, get client, and get bucket
        credentialsFile = NamedTemporaryFile(mode='wb', buffering=0, suffix='.json')
        credentialsFile.write(credentials)
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentialsFile.name
        client = storage.Client()
        bucket = client.get_bucket(bucketName)
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = ''
        credentialsFile.close()

        while True:
            task = taskQueue.get()
            if task is None:
                return

            name: str = task

            bucket.delete_blob(name)


if __name__ == '__main__':
    print('The entry point of this program is in commandline.py')
    print('Use command \'python3 commandline.py -h\'')
