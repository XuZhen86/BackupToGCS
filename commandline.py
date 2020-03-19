import logging
import sys
from argparse import ArgumentParser
from multiprocessing import cpu_count

from action import Action
from backupcommand import BackupCommand
from cloudstorage import CloudStorage
from database import Database


class CommandLine:
    def __init__(self):
        args = self.parseArgs()
        self.setLogLevel(args.log)
        self.dispatchCommand(args)

    def close(self) -> None:
        return

    def parseArgs(self):
        parser = ArgumentParser(
            description='''
                A python program that automatically slices, encrypts, and backup files to Google Cloud Storage.
                During backup, files are sliced into chunks of maximun 32MiB.
                Each chunks encrypted with unique keys using Fernet encryption.
                Then uploaded to specified Google Cloud Storage bucket.
                Information of files and slices are stored in a SQLite3 database file.
                During restore, slices are downloaded, decrypted, then assembled into original file with specified path.
            ''',
            epilog='''
                Visit https://github.com/XuZhen86/BackupToGCS to view this project on GitHub.
            '''
        )

        parser.add_argument(
            '-f', '--file', action='store', default='./database.db',
            help='''
                SQLite3 database file containing all necessary data.
                ATTENTION: Database file is not encrypted and contains sensitive data.
                (default: ./database.db)
            '''
        )
        parser.add_argument(
            '--log', action='store', default='',
            help='''
                Set log level.
            '''
        )
        parser.add_argument(
            '--nProcesses', action='store', default='2',
            help='''
                Set number of processes used for uploading in parallel.
                (default: 2)
            '''
        )
        parser.add_argument(
            '--queueSize', action='store', default=8,
            help='''
                Set max number of pending uploading tasks.
                (default: 8)
            '''
        )

        subparsers = parser.add_subparsers(
            help='commands',
            dest='command',
            required=True
        )

        subparsers.add_parser(
            'noop',
            help='''
                No operation, do nothing and exit.
                Reserved for development purposes.
            '''
        )

        backupParser = subparsers.add_parser(
            'backup',
            help='''
                Backup files in specified path.
            '''
        )
        backupParser.add_argument(
            'path', type=str,
            help='''
                Path containing files to be backed-up.
            '''
        )
        backupParser.add_argument(
            '--nEncryptionWorkers', default=cpu_count(), type=int,
            help='''
                Set number of processes used for encryption.
                (default: {})
            '''.format(cpu_count())
        )
        backupParser.add_argument(
            '--nUploadWorkers', default=cpu_count(), type=int,
            help='''
                Set number of processes used for uploading.
                The same number of processes will be created to handle removal of objects.
                (default: {})
            '''.format(cpu_count())
        )
        backupParser.add_argument(
            '--nUploadPending', default=cpu_count()*2, type=int,
            help='''
                Set maximun number of pending uploads.
                Pay attention to memory usage when setting this argument.
                (default: {})
            '''.format(cpu_count()*2)
        )

        restoreParser = subparsers.add_parser(
            'restore',
            help='''
                Restore files in specified path.
            '''
        )
        restoreParser.add_argument(
            'path', action='store',
            help='''
                Path containing files to be restored.
            '''
        )
        restoreParser.add_argument(
            '--swapPrefix', action='store', nargs=2,
            help='''
                Replace the prefix of file paths with the new one.
                Usually used when restoring to a path other than the one used for backup.
                Example: --swapPrefix /home/user1/oldPath /home/user2/newPath
            '''
        )

        removeParser = subparsers.add_parser(
            'remove',
            help='''
                Remove files in path from backup list and remove relevant data from remote bucket.
            '''
        )
        removeParser.add_argument(
            '-f', '--force', action='store_true', default=False,
            help='''
                Attempt to remove the files without prompting for confirmation.
                (default: false)
            '''
        )
        removeParser.add_argument(
            'path', action='store',
            help='''
                Path containing files to be removed.
            '''
        )

        listParser = subparsers.add_parser(
            'list',
            help='''
                Print info of files under specified path, similar to Unix/Linux `ls` command.
            '''
        )
        listParser.add_argument(
            'path', action='store',
            help='''
                Path containing files to be printed.
            '''
        )
        listParser.add_argument(
            '-m', '--machineReadable', action='store_true', default=False,
            help='''
                Print in a machine-readable format.
            '''
        )

        purgeRemoteParser = subparsers.add_parser(
            'purgeRemote',
            help='''
                List or remove unused objects in remote bucket.
                Usually used to sanitize remote bucket after interrupting a backup command.
            '''
        )
        purgeRemoteParser.add_argument(
            '-r', '--remove', action='store_true', default=False,
            help='''
                Remove unused objects.
                (default: false)
            '''
        )

        setupParser = subparsers.add_parser(
            'setup',
            help='''
                First time setup to SQLite3 database file containing all necessary data.
            '''
        )
        setupParser.add_argument(
            '-f', '--file', action='store', default='./database.db',
            help='''
                SQLite3 database file name that new config will be written to.
                (default: ./database.db)
            '''
        )
        setupParser.add_argument(
            '-c', '--credentials', action='store', default='./credentials.json',
            help='''
                Credentials file supplied by Google.
                This file is only need for the setup and a copy of it is saved to the database file.
                (default: ./credentials.json)
                (https://cloud.google.com/docs/authentication/getting-started)
            '''
        )
        setupParser.add_argument(
            '-b', '--bucket', action='store', default='backup',
            help='''
                Bucket name that stores backup files.
                This name is only need for the setup and a copy of it is saved to the database file.
                (default: backup)
                (https://cloud.google.com/storage/docs/naming)
            '''
        )

        args = parser.parse_args(sys.argv[1:])
        return args

    def setLogLevel(self, logLevel: str) -> None:
        numericLevel = getattr(logging, logLevel.upper(), None)
        if isinstance(numericLevel, int):
            logging.basicConfig(level=numericLevel)

    def dispatchCommand(self, args) -> None:
        if args.command == 'noop':
            print(args)
            return

        if args.command == 'setup':
            database = Database(args.file)
            CloudStorage.setCredentials(database, args.credentials)
            CloudStorage.setBucketName(database, args.bucket)
            database.commit()
            database.close()
            return

        action = Action(args.file, int(args.nProcesses), int(args.queueSize))
        try:
            if args.command == 'backup':
                database = Database(args.file)
                backupCommand = BackupCommand(database, args.nEncryptionWorkers, args.nUploadWorkers, args.nUploadPending)
                backupCommand.backupPath(args.path)
                database.commit()
                database.close()

            elif args.command == 'restore':
                action.getPath(args.path, args.swapPrefix)
            elif args.command == 'remove':
                action.removePath(args.path, args.force)
            elif args.command == 'list':
                action.listFiles(args.path, args.machineReadable)
            elif args.command == 'purgeRemote':
                action.purgeRemote(args.remove)
            else:
                print('Unknown command: {}'.format(args.command))
            action.close()
        except KeyboardInterrupt:
            print('\nReceived KeyboardInterrupt, changes to database are not committed.')
            action.close(commitDatabase=False, waitForTasks=False)


if __name__ == '__main__':
    cl = CommandLine()
    cl.close()
