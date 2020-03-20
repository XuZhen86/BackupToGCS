import json
import os
import sqlite3
from sqlite3 import Connection
from typing import List, Optional


class FileInfo:
    def __init__(self, path: str, sha256: bytes, stats: dict, encryptedSize: int, decryptedSize: int, blobIds: List[int]):
        self.path = path
        self.sha256 = sha256
        self.stats = stats
        self.encryptedSize = encryptedSize
        self.decryptedSize = decryptedSize
        self.blobIds = blobIds

    def __str__(self):
        return str([
            self.path,
            self.sha256,
            self.stats,
            self.encryptedSize,
            self.decryptedSize,
            self.blobIds
        ])

    def toSqlList(self):
        return [
            self.path,
            self.sha256,
            self.stats['mode'],
            self.stats['uid'],
            self.stats['gid'],
            self.stats['atime'],
            self.stats['mtime'],
            self.encryptedSize,
            self.decryptedSize,
            json.dumps(self.blobIds)
        ]

    @classmethod
    def fromSqlList(cls, sqlList: List) -> object:
        return FileInfo(
            sqlList[0],
            sqlList[1], {
                'mode': sqlList[2],
                'uid': sqlList[3],
                'gid': sqlList[4],
                'atime': sqlList[5],
                'mtime': sqlList[6]
            },
            sqlList[7],
            sqlList[8],
            json.loads(sqlList[9])
        )


class BlobInfo:
    def __init__(self, name: str, encryptionKey: bytes, encryptedSize: int, encryptedSha256: bytes, decryptedSize: int, decryptedSha256: bytes):
        self.name = name
        self.encryptionKey = encryptionKey
        self.encryptedSize = encryptedSize
        self.encryptedSha256 = encryptedSha256
        self.decryptedSize = decryptedSize
        self.decryptedSha256 = decryptedSha256

    def __str__(self):
        return str([
            self.name,
            self.encryptionKey,
            self.encryptedSize,
            self.encryptedSha256,
            self.decryptedSize,
            self.decryptedSha256
        ])

    def toSqlList(self):
        return [
            self.name,
            self.encryptionKey,
            self.encryptedSize,
            self.encryptedSha256,
            self.decryptedSize,
            self.decryptedSha256
        ]

    @classmethod
    def fromSqlList(cls, sqlList: List) -> object:
        return BlobInfo(
            sqlList[0],
            sqlList[1],
            sqlList[2],
            sqlList[3],
            sqlList[4],
            sqlList[5]
        )


class Database:
    def __init__(self, dbFileName: str, connection: Connection = None):
        self.sqls = {
            # Create table
            'createFilesTable': '''
                create table if not exists Files (
                    id              integer not null    primary key     autoincrement,
                    path            text    not null    unique,
                    sha256          blob    not null,
                    mode            integer not null,
                    uid             integer not null,
                    gid             integer not null,
                    atime           real    not null,
                    mtime           real    not null,
                    encryptedSize   integer not null,
                    decryptedSize   integer not null,
                    blobIds         text    not null    unique
                )
            ''',
            'createBlobsTable': '''
                create table if not exists Blobs (
                    id              integer not null    primary key     autoincrement,
                    name            text    not null    unique,
                    encryptionKey   blob    not null    unique,
                    encryptedSize   integer not null,
                    encryptedSha256 blob    not null,
                    decryptedSize   integer not null,
                    decryptedSha256 blob    not null
                )
            ''',
            'createPairsTable': '''
                create table if not exists Pairs (
                    id              integer not null    primary key     autoincrement,
                    key             text    not null    unique,
                    value           text    not null
                )
            ''',

            # Files
            'deleteFile': '''
                delete from Files
                where   path = ?
            ''',
            'insertFile': '''
                insert into Files   (path, sha256, mode, uid, gid, atime, mtime, encryptedSize, decryptedSize, blobIds)
                values              (?,    ?,      ?,    ?,   ?,   ?,     ?,     ?,             ?,             ?)
            ''',
            'selectFile': '''
                select  path, sha256, mode, uid, gid, atime, mtime, encryptedSize, decryptedSize, blobIds
                from    Files
                where   path = ?
            ''',
            'updateFile': '''
                update  Files
                set     path = ?, sha256 = ?, mode = ?, uid = ?, gid = ?, atime = ?, mtime = ?, encryptedSize = ?, decryptedSize = ?, blobIds = ?
                where   path = ?
            ''',

            # Blobs
            'deleteBlob': '''
                delete from Blobs
                where   id = ? or name = ?
            ''',
            'insertBlob': '''
                insert into Blobs   (name, encryptionKey, encryptedSize, encryptedSha256, decryptedSize, decryptedSha256)
                values              (?,    ?,             ?,             ?,               ?,             ?)
            ''',
            'selectBlob': '''
                select  name, encryptionKey, encryptedSize, encryptedSha256, decryptedSize, decryptedSha256
                from    Blobs
                where   id = ? or name = ?
            ''',
            'updateBlob': '''
                update  Blobs
                set     name = ?, encryptionKey = ?, encryptedSize = ?, encryptedSha256 = ?, decryptedSize = ?, decryptedSha256 = ?
                where   id = ? or name = ?
            ''',

            # Pairs
            'deletePair': '''
                delete from Pairs
                where   key = ?
            ''',
            'insertPair': '''
                insert into Pairs   (key, value)
                values              (?,   ?)
            ''',
            'selectPair': '''
                select  value
                from    Pairs
                where   key = ?
            ''',
            'updatePair': '''
                update  Pairs
                set     value = ?
                where   key = ?
            ''',

            # Select Prefix
            'selectPaths': '''
                select path
                from Files
            ''',
            'selectBlobs': '''
                select name
                from Blobs
            '''
        }

        # If a database connection is given, use the connection
        if connection is not None:
            self.db = connection
        # Otherwise create connection to file
        else:
            # A new file is created if file does not exist
            self.db = sqlite3.connect(dbFileName)

        # Create tables
        self.createTables()

    def commit(self) -> None:
        self.db.commit()

    def close(self) -> None:
        self.db.close()

    def createTables(self) -> None:
        cursor = self.db.cursor()
        cursor.execute(self.sqls['createFilesTable'])
        cursor.execute(self.sqls['createBlobsTable'])
        cursor.execute(self.sqls['createPairsTable'])
        cursor.close()

    def setFile(self, fileInfo: FileInfo) -> int:
        cursor = self.db.cursor()
        cursor.execute(self.sqls['selectFile'], [fileInfo.path])

        if cursor.fetchone() is None:
            cursor.execute(self.sqls['insertFile'], fileInfo.toSqlList())
        else:
            cursor.execute(self.sqls['updateFile'], fileInfo.toSqlList() + [fileInfo.path])

        cursor.close()
        return cursor.lastrowid

    def getFile(self, path: str) -> Optional[FileInfo]:
        cursor = self.db.cursor()
        cursor.execute(self.sqls['selectFile'], [path])
        fileInfoSqlList = cursor.fetchone()

        if fileInfoSqlList is None:
            return None

        cursor.close()
        return FileInfo.fromSqlList(fileInfoSqlList)

    def removeFile(self, path: str) -> bool:
        cursor = self.db.cursor()
        cursor.execute(self.sqls['selectFile'], [path])
        fileInfoSqlList = cursor.fetchone()

        if fileInfoSqlList is None:
            return False

        blobIds = (FileInfo.fromSqlList(fileInfoSqlList)).blobIds
        for blobId in blobIds:
            self.removeBlob(blobId)

        cursor.execute(self.sqls['deleteFile'], [path])

        cursor.close()
        return True

    def setBlob(self, blobInfo: BlobInfo) -> int:
        cursor = self.db.cursor()
        cursor.execute(self.sqls['selectBlob'], [0, blobInfo.name])

        if cursor.fetchone() is None:
            cursor.execute(self.sqls['insertBlob'], blobInfo.toSqlList())
        else:
            cursor.execute(self.sqls['updateBlob'], blobInfo.toSqlList() + [0, blobInfo.name])

        cursor.close()
        return cursor.lastrowid

    def getBlob(self, blobId: int) -> Optional[BlobInfo]:
        cursor = self.db.cursor()
        cursor.execute(self.sqls['selectBlob'], [blobId, ''])
        blobInfoSqlList = cursor.fetchone()

        if blobInfoSqlList is None:
            return None

        cursor.close()
        return BlobInfo.fromSqlList(blobInfoSqlList)

    def removeBlob(self, blobId: int) -> bool:
        cursor = self.db.cursor()
        cursor.execute(self.sqls['selectBlob'], [blobId, ''])
        blobInfoSqlList = cursor.fetchone()

        if blobInfoSqlList is None:
            return False

        cursor.execute(self.sqls['deleteBlob'], [blobId, ''])

        cursor.close()
        return True

    def setPair(self, pairKey: str, pairValue: str) -> int:
        cursor = self.db.cursor()
        cursor.execute(self.sqls['selectPair'], [pairKey])

        if cursor.fetchone() is None:
            cursor.execute(self.sqls['insertPair'], [pairKey, pairValue])
        else:
            cursor.execute(self.sqls['updatePair'], [pairValue, pairKey])

        cursor.close()
        return cursor.lastrowid

    def getPair(self, pairKey: str) -> Optional[str]:
        cursor = self.db.cursor()
        cursor.execute(self.sqls['selectPair'], [pairKey])
        pairValueSqlList = cursor.fetchone()

        if pairValueSqlList is None:
            return None

        cursor.close()
        return pairValueSqlList[0]

    def removePair(self, pairKey: str) -> bool:
        cursor = self.db.cursor()
        cursor.execute(self.sqls['selectPair'], [pairKey])

        pairValueSqlList = cursor.fetchone()

        if pairValueSqlList is None:
            return False

        cursor.execute(self.sqls['deletePair'], [pairKey])

        cursor.close()
        return True

    def selectPaths(self, prefix: str) -> List[str]:
        cursor = self.db.cursor()
        cursor.execute(self.sqls['selectPaths'])
        pathsValueSqlList = cursor.fetchall()
        cursor.close()

        return filter(
            lambda path: os.path.commonprefix([path, prefix]) == prefix,
            map(
                lambda pathTuple: pathTuple[0],
                pathsValueSqlList
            )
        )

    def selectBlobs(self, prefix: str) -> List[int]:
        cursor = self.db.cursor()
        cursor.execute(self.sqls['selectBlobs'])
        namesValueSqlList = cursor.fetchall()
        cursor.close()

        return filter(
            lambda name: name.startswith(prefix),
            map(
                lambda nameTuple: nameTuple[0],
                namesValueSqlList
            )
        )

    @staticmethod
    def getTransientCopy(dbFileName: str):
        # Open file database in readonly mode
        # https://docs.python.org/3/library/sqlite3.html#sqlite3.connect
        fileDb = sqlite3.connect('file:{}?mode=ro'.format(dbFileName), uri=True)
        # Open in-memory database
        memoryDb = sqlite3.connect(':memory:')

        # Copy content over to memory
        fileDb.backup(memoryDb)

        # Close file database
        fileDb.close()

        # Create Database object and return
        return Database(None, connection=memoryDb)

if __name__ == '__main__':
    print('The entry point of this program is in commandline.py')
    print('Use command \'python3 commandline.py -h\'')
