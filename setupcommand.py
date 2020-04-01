import json
import os

from google.cloud import storage
from google.oauth2.service_account import Credentials

from database import Database


def setupCommand(
    databaseFileName: str,
    credentialsFileName: str,
    bucketName: str
) -> None:
    # Verify credentials and bucket name
    credentialsJson = json.load(open(credentialsFileName, 'r'))
    credentials = Credentials.from_service_account_info(credentialsJson)
    client = storage.Client(project=credentials.project_id, credentials=credentials)
    client.get_bucket(bucketName)

    # Open database
    database = Database(databaseFileName)

    # Write credentials and bucket name to database
    database.setPair('credentials', json.dumps(credentialsJson))
    database.setPair('bucketName', bucketName)

    # Close database
    database.commit()
    database.close()
