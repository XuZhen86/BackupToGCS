from argparse import ArgumentParser
import sys
import logging
from action import Action
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
        parser = ArgumentParser()

        parser.add_argument(
            '-f', '--file', action='store', default='database.db',
            help='''
                SQLite3 database file containing all necessary data.
                ATTENTION: Database file is not encrypted and contains sensitive data.
                (default: database.db)
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
            'path', action='store',
            help='''
                Path containing files to be backed-up.
            '''
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

        removeParser = subparsers.add_parser(
            'remove',
            help='''
                Remove files in path from backup list and remove relevant data from remote bucket.
            '''
        )
        removeParser.add_argument(
            'path', action='store',
            help='''
                Path containing files to be removed.
            '''
        )

        lsParser = subparsers.add_parser(
            'ls',
            help='''
                Print info of files under specified path, similar to Unix/Linux `ls` command.
            '''
        )
        lsParser.add_argument(
            'path', action='store',
            help='''
                Path containing files to be printed.
            '''
        )
        lsParser.add_argument(
            '-m', '--machineReadable', action='store_true', default=False,
            help='''
                Print in a machine-readable format.
            '''
        )

        purgeRemoteParser = subparsers.add_parser(
            'purgeRemote',
            help='''
                List unused objects in remote bucket.
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
            '-f', '--file', action='store', default='database.db',
            help='''
                SQLite3 database file name that new config will be written to.
                (default: database.db)
            '''
        )
        setupParser.add_argument(
            '-c', '--credentials', action='store', default='credentials.json',
            help='''
                Credentials file supplied by Google.
                This file is only need for the setup and a copy of it is saved to the database file.
                (default: credentials.json)
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

        if args.command == 'backup':
            action = Action(args.file, int(
                args.nProcesses), int(args.queueSize))
            action.setPath(args.path)
            action.close()
            return

        if args.command == 'restore':
            action = Action(args.file, int(
                args.nProcesses), int(args.queueSize))
            action.getPath(args.path)
            action.close()
            return

        if args.command == 'remove':
            action = Action(args.file, int(
                args.nProcesses), int(args.queueSize))
            action.removePath(args.path)
            action.close()
            return

        if args.command == 'ls':
            action = Action(args.file, int(
                args.nProcesses), int(args.queueSize))
            action.listFiles(args.path, args.machineReadable)
            action.close()
            return

        if args.command == 'purgeRemote':
            action = Action(args.file, int(
                args.nProcesses), int(args.queueSize))
            nBlobs = action.purgeRemote(args.remove)
            action.close()

            if args.remove:
                print('Removed {} remote objects.'.format(nBlobs))
            else:
                print('Found {} unused remote objects.'.format(nBlobs))
            return

        if args.command == 'setup':
            database = Database(args.file)
            CloudStorage.setCredentials(database, args.credentials)
            CloudStorage.setBucketName(database, args.bucket)
            database.close()
            return


if __name__ == '__main__':
    cl = CommandLine()
    cl.close()
