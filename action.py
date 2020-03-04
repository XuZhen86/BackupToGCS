import hashlib
import logging
import os
import random
from datetime import datetime

import humanize
from cryptography.fernet import Fernet

from cloudstorage import CloudStorage
from database import BlobInfo, Database, FileInfo


class Action:
    def __init__(self, dbFileName: str, nProcesses: int = 2, queueSize: int = 8):
        self.logger = logging.getLogger(__name__)

        self.database = Database(dbFileName)
        self.cloudStorage = CloudStorage(self.database)

    def close(self) -> None:
        self.cloudStorage.close()
        self.database.close()
        self.logger.debug('Closed')

    def setBlob(self, data: bytes) -> int:
        decryptedSize = len(data)
        decryptedSha256 = hashlib.sha256(data).digest()

        encryptionKey = Fernet.generate_key()
        data = Fernet(encryptionKey).encrypt(data)

        encryptedSize = len(data)
        encryptedSha256 = hashlib.sha256(data).digest()

        name = ''.join(random.choice('0123456789abcdef') for i in range(32))
        self.cloudStorage.setBlob(name, data)
        blobId = self.database.setBlob(BlobInfo(name, encryptionKey, encryptedSize, encryptedSha256, decryptedSize, decryptedSha256))

        self.logger.info('+ {}'.format(name))
        self.logger.debug('Set blob={} id={} dSize={} eSize={}'.format(name, blobId, decryptedSize, encryptedSize))
        return blobId

    def getBlob(self, blobId: int) -> bytes:
        blobInfo = self.database.getBlob(blobId)

        data = self.cloudStorage.getBlob(blobInfo.name)
        assert blobInfo.encryptedSize == len(data)
        assert blobInfo.encryptedSha256 == hashlib.sha256(data).digest()

        data = Fernet(blobInfo.encryptionKey).decrypt(data)
        assert blobInfo.decryptedSize == len(data)
        assert blobInfo.decryptedSha256 == hashlib.sha256(data).digest()

        self.logger.info('= {}'.format(blobInfo.name))
        self.logger.debug('Get blob={} id={} dSize={} eSize={}'.format(blobInfo.name, blobId, blobInfo.decryptedSize, blobInfo.encryptedSize))
        return data

    def removeBlob(self, blobId: int) -> bool:
        blobInfo = self.database.getBlob(blobId)
        if blobInfo is None:
            return False

        self.cloudStorage.removeBlob(blobInfo.name)
        self.database.removeBlob(blobId)

        self.logger.info('- {}'.format(blobInfo.name))
        self.logger.debug('Remove blob={} id={} dSize={} eSize={}'.format(blobInfo.name, blobId, blobInfo.decryptedSize, blobInfo.encryptedSize))
        return True

    def setFile(self, path: str) -> int:
        osStat = os.stat(path)
        stats = {
            'mode': osStat.st_mode,
            'uid': osStat.st_uid,
            'gid': osStat.st_gid,
            'atime': osStat.st_atime,
            'mtime': osStat.st_mtime,
        }

        if not self.isFileChanged(path):
            return 0
        self.removeFile(path)

        sha256 = hashlib.sha256()
        blobIds = []
        encryptedSize = 0
        decryptedSize = 0

        fp = open(path, 'rb')
        for data in iter(lambda: fp.read(1024*1024*32), b''):
            sha256.update(data)
            blobIds.append(self.setBlob(data))
            encryptedSize += self.database.getBlob(blobIds[-1]).encryptedSize
            decryptedSize += len(data)

        fileId = self.database.setFile(FileInfo(path, sha256.digest(), stats, encryptedSize, decryptedSize, blobIds))

        self.logger.info('+ {}'.format(path))
        self.logger.debug('Set file={} dSize={} eSize={} nBlobs={}'.format(path, decryptedSize, encryptedSize, len(blobIds)))
        return fileId

    def getFile(self, path: str) -> bool:
        fileInfo = self.database.getFile(path)
        if fileInfo is None:
            return False

        decryptedSize = 0
        sha256 = hashlib.sha256()

        os.makedirs(os.path.dirname(path), exist_ok=True)

        fp = open(path, 'wb')
        for blobId in fileInfo.blobIds:
            data = self.getBlob(blobId)
            decryptedSize += len(data)
            sha256.update(data)
            fp.write(data)
        fp.close()

        assert fileInfo.decryptedSize == decryptedSize
        assert fileInfo.sha256 == sha256.digest()

        stats = fileInfo.stats
        os.chmod(path, stats['mode'])
        os.chown(path, stats['uid'], stats['gid'])
        os.utime(path, (stats['atime'], stats['mtime']))

        self.logger.info('= {}'.format(path))
        self.logger.debug('Get file={} dSize={} eSize={} nBlobs={}'.format(path, fileInfo.decryptedSize, fileInfo.encryptedSize, len(fileInfo.blobIds)))
        return True

    def removeFile(self, path: str) -> bool:
        fileInfo = self.database.getFile(path)
        if fileInfo is None:
            return False

        for blobId in fileInfo.blobIds:
            assert self.removeBlob(blobId)
        assert self.database.removeFile(path)

        self.logger.info('- {}'.format(path))
        self.logger.debug('Remove file={} dSize={} eSize={} nBlobs={}'.format(path, fileInfo.decryptedSize, fileInfo.encryptedSize, len(fileInfo.blobIds)))
        return True

    def setPath(self, relPath: str) -> None:
        absPath = os.path.abspath(relPath)

        if os.path.isfile(absPath):
            self.setFile(absPath)
            return

        for dirPath, _, fileNames in os.walk(absPath):
            for fileName in fileNames:
                filePath = os.path.join(dirPath, fileName)
                self.setFile(filePath)

    def getPath(self, relPath: str, swapPrefix: list = ['/', '/']) -> None:
        absPath = os.path.abspath(relPath)
        swapPrefix[0] = os.path.abspath(swapPrefix[0])
        swapPrefix[1] = os.path.abspath(swapPrefix[1])
        if absPath.startswith(swapPrefix[0]):
            absPath = swapPrefix[1] + absPath[len(swapPrefix[0]):]

        if os.path.isfile(absPath):
            self.getFile(absPath)
            return

        for path in self.database.selectPaths(absPath):
            assert self.getFile(path)

    def removePath(self, relPath: str) -> None:
        absPath = os.path.abspath(relPath)

        for path in self.database.selectPaths(absPath):
            assert self.removeFile(path)

    def isFileChanged(self, path) -> bool:
        fileInfo = self.database.getFile(path)
        if fileInfo is None:
            return True

        stat = os.stat(path)
        if fileInfo.stats['mtime'] == stat.st_mtime:
            return False

        if fileInfo.decryptedSize != stat.st_size:
            return True

        fp = open(path, 'rb')
        for index, data in enumerate(iter(lambda: fp.read(1024*1024*32), b'')):
            blobInfo = self.database.getBlob(fileInfo.blobIds[index])
            assert blobInfo is not None
            if blobInfo.decryptedSha256 != hashlib.sha256(data).digest():
                return True

        return False

    def listFiles(self, relPath: str, machineReadable: bool) -> None:
        absPath = os.path.abspath(relPath)

        if not machineReadable:
            print('{:8} {:5} {:5} {:5} {:10} {:10} {:26} {}'.format(
                'Perm', 'n', 'Uid', 'Gid', 'DecSize', 'EncSize', 'ModTime', 'Path'
            ))

        for path in self.database.selectPaths(absPath):
            fileInfo = self.database.getFile(path)

            output = None
            if machineReadable:
                output = '{}|{}|{}|{}|{}|{}|{}|{}|{}|'.format(
                    oct(fileInfo.stats['mode']),
                    len(fileInfo.blobIds),
                    fileInfo.stats['uid'],
                    fileInfo.stats['gid'],
                    fileInfo.decryptedSize,
                    fileInfo.encryptedSize,
                    fileInfo.stats['atime'],
                    fileInfo.stats['mtime'],
                    path
                )
            else:
                output = '{:8} {:<5} {:<5} {:<5} {:10} {:10} {:26} {}'.format(
                    oct(fileInfo.stats['mode']),
                    len(fileInfo.blobIds),
                    fileInfo.stats['uid'],
                    fileInfo.stats['gid'],
                    humanize.naturalsize(fileInfo.decryptedSize, binary=True),
                    humanize.naturalsize(fileInfo.encryptedSize, binary=True),
                    datetime.fromtimestamp(fileInfo.stats['mtime']).strftime('%c'),
                    path
                )
            print(output)

    def purgeRemote(self, remove: bool) -> int:
        localBlobNames = set(self.database.selectBlobs(''))
        remoteBlobNames = set(self.cloudStorage.getBlobNames())
        setDifference = remoteBlobNames.difference(localBlobNames)

        if remove:
            for name in setDifference:
                self.cloudStorage.removeBlob(name)

        return len(setDifference)


if __name__ == '__main__':
    pass
