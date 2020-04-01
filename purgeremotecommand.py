import json
import logging
import queue
from threading import Thread

import setproctitle
from google.cloud import storage
from google.oauth2.service_account import Credentials
from ttictoc import TicToc

from database import BlobInfo, Database


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

        ticToc.tic()
        if not isTrialRun:
            try:
                bucket.delete_blob(name)
            except:
                pass

        logger.info('× {}, elapsed: {:.3f}s, thread: {}'.format(
            name,
            ticToc.toc(),
            threadId
        ))


def purgeRemoteCommand(
    databaseFileName: str,
    logLevel: str,
    isTrialRun: bool,
    nBlobRemoveThreads: int = 4
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
    logger.info('database: {}, isTrialRun: {}, nBlobRemoveThreads: {}'.format(
        databaseFileName,
        isTrialRun,
        nBlobRemoveThreads
    ))

    # Create queues
    blobRemoveQueue = queue.SimpleQueue()

    # Get database
    database = Database.getTransientCopy(databaseFileName)

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

    # Get GCS client
    credentials = Credentials.from_service_account_info(json.loads(credentials))
    client = storage.Client(project=credentials.project_id, credentials=credentials)

    # Get sets of blob names
    localBlobNames = set(
        database.selectBlobs('')
    )
    remoteBlobNames = set(map(
        lambda blob: blob.name,
        client.list_blobs(bucketName)
    ))

    # Get sets of blob names that exist in remote bucket but not local database
    setDifference = remoteBlobNames.difference(localBlobNames)

    # Send blob names to remove queue
    for blobName in setDifference:
        blobRemoveQueue.put([
            blobName
        ])

    # Stop threads
    for _ in range(nBlobRemoveThreads):
        blobRemoveQueue.put(None)
    for thread in blobRemoveThreads:
        thread.join()
