# Backup To GCS
A tool to automatically slice up, encrypt, and backup to Google Cloud Storage.

## Requirements
* Python >= 3.7. Recommending >= 3.8.
* Pip [humanize](https://pypi.org/project/humanize/) >= 2.0. Used for formatting file size.
* Pip [cryptography](https://pypi.org/project/cryptography/) >= 2.8. Used for encrypting and decrypting data.
* Pip [google-cloud-storage](https://pypi.org/project/google-cloud-storage/) >= 1.26. Used for connecting to Google Cloud Storage.

## Usages
* Use ```python3 commandline.py --help``` to get help.
* Add ```--log=info``` to see what exactly is it doing.
* If you interrupt an on-going process, changes to database will be lost.
* It is recommended to always use absolute path. If you use relative paths, it will be converted to absolute path internally.

### First-time configuration
1. Goto https://cloud.google.com/python to download ```credentials.json``` for your bucket. This file contains sensitive information and should be handled with care.
1. Run ```python3 commandline.py setup -c path/to/credentials.json -b bucketName``` to generate an SQLite3 database file. A ```database.db``` will appear in the folder. You can remove ```credentials.json``` now, since the ```database.db``` contains a copy of ```credentials.json``` and the bucket name.

### Backup a folder or a file to cloud
1. Run ```python3 commandline.py backup path/to/backup``` to start backup process.

### Restore a folder or a file from cloud
1. Run ```python3 commandline.py restore path/to/restore``` to start restore process. The file will be put into its original path.
    * If you want to put the files into a different path, use ```--swapPrefix``` to change the path. For example, a file was originally located in ```/home/xu/Documents/file.txt```, and you want to restore it to ```/home/xu/Desktop/myFolder/file.txt```, use command ```python3 commandline.py restore --swapPrefix /home/xu/Documents /home/xu/Desktop/myFolder /home/xu/Documents/file.txt```. Now ```file.txt``` will be put into ```/home/xu/Desktop/myFolder```.

### Remove backup from database and from cloud
1. Run ```python3 commandline.py remove path/to/remove``` to remove records of backup. This operation does nothing to actual files in the folder. It only removes record in database and erase data from cloud bucket.

### List file information
1. Run ```python3 commandline.py list path/to/list``` to list all files and related information.
    * To list all files, use command ```python3 commandline.py list /```.

### Remove unused files from cloud.
Usually used to sanitize remote bucket after interrupting a backup command.
1. Run ```python3 commandline.py purgeRemove``` to see how many objects are unused. This operation does not remove objects.
    * Add ```-r``` to actually remove them.

## Advanced options
Only tune these options if you know what are you doing.

### Set database file to be used
1. Add ```--file fileName```. For example, use ```--file ../database.db``` to use database file in another folder.
    * Default ```./database.db```

### Set number of processes used for uploading
1. Add ```--nProcesses number```. For example, use ```--nProcesses 8``` to upload maximum of 8 blobs in parallel.
    * Default 2 processes.
    * It uses producer-consumer pattern internally.

### Set number of pending uploading tasks
1. Add ```--queueSize number```. For example, use ```--queueSize 100``` to allow maximum of 100 waiting uploads.
    * Default 8 tasks.
    * Pay attention to memory usage. In theory, each pending task takes about 32MB of memory until they are processed.

## Disclaimer
1. This project is intended for experimental and educational purposes. It is not tested and/or rated for use in production environment.
1. The author is not responsible, including but not limited to:
    * any loss of data.
    * any incurred Google Cloud Service bills.
