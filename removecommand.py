import json
import logging
import os
import queue
from functools import partial
from threading import Thread

import setproctitle
from cryptography.fernet import Fernet
from google.cloud import storage
from google.oauth2.service_account import Credentials
from humanize import naturalsize
from ttictoc import TicToc

from database import BlobInfo, Database, FileInfo

naturalsize = partial(naturalsize, binary=True)


def setLogingLevel(
    logLevel: str
) -> None:
    numericLevel = getattr(logging, logLevel.upper(), None)
    if isinstance(numericLevel, int):
        logging.basicConfig(level=numericLevel)


def BlobRemoveThread(
    blobRemoveQueue: queue.SimpleQueue,
    credentials: str,
    bucketName: str,
    isTrialRun: bool,
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
        # Get task
        task = blobRemoveQueue.get()
        if task is None:
            break

        # Extract task
        name: str = task[0]
        size: str = task[1]
        index: int = task[2]
        nBlobs: int = task[3]

        ticToc.tic()
        if not isTrialRun:
            try:
                bucket.delete_blob(name)
            except:
                pass

        logger.info('× {}, elapsed: {:.3f}s, size: {}, blob: {} of {}, thread: {}'.format(
            name,
            ticToc.toc(),
            naturalsize(size),
            index+1,
            nBlobs,
            threadId
        ))


def removeCommand(
    databaseFileName: str,
    prefix: str,
    logLevel: str,
    isTrialRun: bool,
    nBlobRemoveThreads: int = 3
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

    # Print parameters
    logger.info('database: {}, prefix: {}, isTrialRun: {}, nBlobRemoveThreads: {}'.format(
        databaseFileName,
        prefix,
        isTrialRun,
        nBlobRemoveThreads
    ))

    # Create queues
    blobRemoveQueue = queue.SimpleQueue()

    # Get database
    # Get transient database if trial run
    if isTrialRun:
        database = Database.getTransientCopy(databaseFileName)
    else:
        database = Database(databaseFileName)

    # Get credentials file and bucket name
    result = getGcsParameters(databaseFileName)
    credentials: str = result[0]
    bucketName: str = result[1]

    # Create blob remove threads
    blobRemoveThreads = []
    for threadId in range(nBlobRemoveThreads):
        thread = Thread(
            target=BlobRemoveThread,
            args=[
                blobRemoveQueue,
                credentials,
                bucketName,
                isTrialRun,
                logLevel,
                threadId
            ],
            name='BlobRemoveThread{}'.format(threadId)
        )
        blobRemoveThreads.append(thread)

    # Start threads
    for thread in blobRemoveThreads:
        thread.start()

    # Convert to absolute path
    prefix = os.path.abspath(prefix)

    # Get list of files to be removed
    paths = database.selectPaths(prefix)

    # Create stats variables
    ticToc = TicToc()

    # Remove every files and their blobs
    for path in paths:
        ticToc.tic()
        fileInfo = database.getFile(path)

        # For each blob of the file
        for index, blobId in enumerate(fileInfo.blobIds):
            blobInfo = database.getBlob(blobId)

            # Remove blob from cloud bucket
            blobRemoveQueue.put([
                blobInfo.name,
                blobInfo.encryptedSize,
                index,
                len(fileInfo.blobIds)
            ])

            # Remove blob from database
            database.removeBlob(blobId)

        # Remove file from database
        database.removeFile(path)

        logger.info('× {}, elapsed: {:.3f}, size: {} {}, blobs: {}'.format(
            path,
            ticToc.toc(),
            naturalsize(fileInfo.decryptedSize),
            naturalsize(fileInfo.encryptedSize),
            len(fileInfo.blobIds)
        ))

    # Commit and close database
    database.commit()
    database.close()

    # Wait for threads to exit
    for _ in range(nBlobRemoveThreads):
        blobRemoveQueue.put(None)
    for thread in blobRemoveThreads:
        thread.join()


if __name__ == '__main__':
    print('The entry point of this program is in commandline.py')
    print('Use command \'python3 commandline.py -h\'')
