import json
import os
from multiprocessing import JoinableQueue, Process, Queue
from tempfile import NamedTemporaryFile
from typing import List, Optional

from google.cloud import exceptions, storage

from database import Database


class CloudStorage:
    def __init__(self, db: Database, nProcesses: int = 2, queueSize: int = 8):
        self.credentialsFile = NamedTemporaryFile(mode='wb', buffering=0, suffix='.json')
        self.credentialsFile.write(db.getPair('credentials').encode())
        bucketName = db.getPair('bucketName')

        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = self.credentialsFile.name
        self.client = storage.Client()
        self.bucket = self.client.get_bucket(bucketName)
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = ''

        self.processes = []
        self.uploadQueue = JoinableQueue(queueSize)
        for processId in range(nProcesses):
            process = Process(
                target=CloudStorage.uploadWorker,
                name='UploadProcess{}'.format(processId),
                args=(self.credentialsFile.name, bucketName, self.uploadQueue)
            )
            process.start()
            self.processes.append(process)

    def close(self) -> None:
        self.uploadQueue.join()
        for _ in range(len(self.processes)):
            self.uploadQueue.put(None)
        for process in self.processes:
            process.join()
        self.credentialsFile.close()

    @staticmethod
    def uploadWorker(credentialsFileName: str, bucketName: str, queue: JoinableQueue) -> None:
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentialsFileName
        bucket = storage.Client().get_bucket(bucketName)
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = ''

        while True:
            task = queue.get()
            if task is None:
                return

            name, data = task
            bucket.blob(name).upload_from_string(data)
            queue.task_done()

    @staticmethod
    def setCredentials(db: Database, credentialsPath: str) -> None:
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentialsPath
        storage.Client()
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = ''

        credentials = json.load(open(credentialsPath, 'r'))
        db.setPair('credentials', json.dumps(credentials))

    @staticmethod
    def setBucketName(db: Database, bucketName: str) -> None:
        credentialsFile = NamedTemporaryFile(mode='wb', buffering=0, suffix='.json')
        credentialsFile.write(db.getPair('credentials').encode())

        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentialsFile.name
        client = storage.Client()
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = ''

        client.get_bucket(bucketName)

        db.setPair('bucketName', bucketName)

    def setBlob(self, name: str, data: bytes) -> None:
        self.uploadQueue.put([name, data])

    def getBlob(self, name: str) -> Optional[bytes]:
        self.uploadQueue.join()

        blob = self.bucket.get_blob(name)
        if blob is None:
            return None
        return blob.download_as_string()

    def removeBlob(self, name: str) -> bool:
        self.uploadQueue.join()

        try:
            self.bucket.delete_blob(name)
        except exceptions.NotFound:
            return False
        return True

    def getBlobNames(self) -> List[str]:
        self.uploadQueue.join()
        return map(
            lambda blob: blob.name,
            self.client.list_blobs(self.bucket.name)
        )


if __name__ == '__main__':
    pass
