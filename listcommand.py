import logging
import os
import queue
from datetime import datetime
from functools import partial

import setproctitle
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


def listCommand(
    databaseFileName: str,
    prefix: str,
    logLevel: str,
    sortBy: str,
    reverseSort: bool,
    machineReadable: bool
) -> None:
    def getKey(
        fileInfo: FileInfo
    ):
        if sortBy == 'perm':
            return fileInfo.stats['mode']
        if sortBy == 'blobs':
            return len(fileInfo.blobIds)
        if sortBy == 'uid':
            return fileInfo.stats['uid']
        if sortBy == 'gid':
            return fileInfo.stats['gid']
        if sortBy == 'decrypt':
            return fileInfo.decryptedSize
        if sortBy == 'encrypt':
            return fileInfo.encryptedSize
        if sortBy == 'modification':
            return fileInfo.stats['mtime']
        if sortBy == 'path':
            return fileInfo.path
        return 0

    # Set process title
    setproctitle.setproctitle('ListCommand')

    # Set logging level and get logger
    setLogingLevel(logLevel)
    logger = logging.getLogger(__name__)

    # Print parameters
    logger.info('database: {}, prefix: {}, sortBy: {}, reverseSort: {}, machineReadable: {}'.format(
        databaseFileName,
        prefix,
        sortBy,
        reverseSort,
        machineReadable
    ))

    # Create stats variables
    ticToc = TicToc()
    ticToc.tic()

    # Get transient database
    database = Database.getTransientCopy(databaseFileName)

    # Get absolute path
    prefix = os.path.abspath(prefix)

    # Get file infos
    fileInfos = [
        database.getFile(path)
        for path in database.selectPaths(prefix)
    ]

    # Close database
    database.close()

    # Sort files based on key
    fileInfos.sort(key=getKey, reverse=reverseSort)

    # Print in different format for machines
    if machineReadable:
        for fileInfo in fileInfos:
            print('{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}'.format(
                fileInfo.stats['mode'],
                len(fileInfo.blobIds),
                fileInfo.stats['uid'],
                fileInfo.stats['gid'],
                fileInfo.decryptedSize,
                fileInfo.encryptedSize,
                fileInfo.stats['atime'],
                fileInfo.stats['mtime'],
                fileInfo.path
            ))
    else:
        # Print a header for humans
        print('{:8} {:5} {:5} {:5} {:10}  {:10}  {:26} {}'.format(
            'Perm', 'Blobs', 'uid', 'gid', 'Decrypt', 'Encrypt', 'Modification', 'Path'
        ))
        for fileInfo in fileInfos:
            print('{:8} {:<5} {:<5} {:<5} {:>10}  {:>10}  {:26} {}'.format(
                oct(fileInfo.stats['mode']),
                len(fileInfo.blobIds),
                fileInfo.stats['uid'],
                fileInfo.stats['gid'],
                naturalsize(fileInfo.decryptedSize),
                naturalsize(fileInfo.encryptedSize),
                datetime.fromtimestamp(fileInfo.stats['mtime']).strftime('%c'),
                fileInfo.path
            ))

    # Print elapsed time
    logger.info('elapsed: {:.3f}s'.format(
        ticToc.toc()
    ))


if __name__ == '__main__':
    print('The entry point of this program is in commandline.py')
    print('Use command \'python3 commandline.py -h\'')
