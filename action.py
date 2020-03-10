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

    def close(self, commitDatabase: bool = True, waitForTasks: bool = True) -> None:
        self.cloudStorage.close(waitForTasks)
        if commitDatabase:
            self.database.commit()
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

        self.logger.info('+ {} {:8} {:10} {:10}'.format(
            name,
            blobId,
            humanize.naturalsize(decryptedSize, binary=True),
            humanize.naturalsize(encryptedSize, binary=True)
        ))
        return blobId

    def getBlob(self, blobId: int) -> bytes:
        blobInfo = self.database.getBlob(blobId)

        data = self.cloudStorage.getBlob(blobInfo.name)
        assert blobInfo.encryptedSize == len(data)
        assert blobInfo.encryptedSha256 == hashlib.sha256(data).digest()

        data = Fernet(blobInfo.encryptionKey).decrypt(data)
        assert blobInfo.decryptedSize == len(data)
        assert blobInfo.decryptedSha256 == hashlib.sha256(data).digest()

        self.logger.info('= {} {:8} {:10} {:10}'.format(
            blobInfo.name,
            blobId,
            humanize.naturalsize(blobInfo.decryptedSize, binary=True),
            humanize.naturalsize(blobInfo.encryptedSize, binary=True)
        ))
        return data

    def removeBlob(self, blobId: int) -> bool:
        blobInfo = self.database.getBlob(blobId)
        if blobInfo is None:
            return False

        self.cloudStorage.removeBlob(blobInfo.name)
        self.database.removeBlob(blobId)

        self.logger.info('- {} {:8} {:10} {:10}'.format(
            blobInfo.name,
            blobId,
            humanize.naturalsize(blobInfo.decryptedSize, binary=True),
            humanize.naturalsize(blobInfo.encryptedSize, binary=True)
        ))
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

        self.logger.info('+ {} {} {} {}'.format(
            path,
            humanize.naturalsize(decryptedSize, binary=True),
            humanize.naturalsize(encryptedSize, binary=True),
            len(blobIds)
        ))
        return fileId

    def getFile(self, path: str, altPath: str = None) -> bool:
        fileInfo = self.database.getFile(path)
        path = altPath if altPath is not None else path

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

        self.logger.info('= {} {} {} {}'.format(
            path,
            humanize.naturalsize(fileInfo.decryptedSize, binary=True),
            humanize.naturalsize(fileInfo.encryptedSize, binary=True),
            len(fileInfo.blobIds)
        ))
        return True

    def removeFile(self, path: str, forceRemove: bool = False) -> bool:
        fileInfo = self.database.getFile(path)
        if fileInfo is None:
            return False

        if forceRemove or input('Remove {}? '.format(path)).lower() == 'yes':
            for blobId in fileInfo.blobIds:
                assert self.removeBlob(blobId)
            assert self.database.removeFile(path)

            self.logger.info('- {} {} {} {}'.format(
                path,
                humanize.naturalsize(fileInfo.decryptedSize, binary=True),
                humanize.naturalsize(fileInfo.encryptedSize, binary=True),
                len(fileInfo.blobIds)
            ))
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

        for path in self.database.selectPaths(absPath):
            altPath = None
            if path.startswith(swapPrefix[0]):
                altPath = swapPrefix[1] + path[len(swapPrefix[0]):]

            assert self.getFile(path, altPath)

    def removePath(self, relPath: str, forceRemove: bool = False) -> None:
        absPath = os.path.abspath(relPath)

        for path in self.database.selectPaths(absPath):
            assert self.removeFile(path, forceRemove)

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

        nBlobs = 0
        decryptedSize = 0
        encryptedSize = 0
        nPaths = 0

        for path in self.database.selectPaths(absPath):
            fileInfo = self.database.getFile(path)

            nBlobs += len(fileInfo.blobIds)
            decryptedSize += fileInfo.decryptedSize
            encryptedSize += fileInfo.encryptedSize
            nPaths += 1

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

        if not machineReadable:
            print('Files: {}. Blobs: {}. Decrypted Size: {}. Encrypted Size: {}. Inflation: {:.2f}%.'.format(
                nPaths,
                nBlobs,
                humanize.naturalsize(decryptedSize, binary=True),
                humanize.naturalsize(encryptedSize, binary=True),
                (encryptedSize - decryptedSize) / decryptedSize * 100 if decryptedSize != 0 else 0
            ))

    def purgeRemote(self, remove: bool) -> int:
        localBlobNames = set(self.database.selectBlobs(''))
        remoteBlobNames = set(self.cloudStorage.getBlobNames())
        setDifference = remoteBlobNames.difference(localBlobNames)

        if remove:
            for name in setDifference:
                self.cloudStorage.removeBlob(name)

        nBlobs = len(setDifference)
        if remove:
            print('Removed {} remote objects.'.format(nBlobs))
        else:
            print('Found {} unused remote objects.'.format(nBlobs))

        return nBlobs


if __name__ == '__main__':
    print('The entry point of this program is in commandline.py')
    print('Use command \'python3 commandline.py -h\'')
