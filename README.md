# MDB-FS - Metadata DB File System

## Overview

A SHA-1 based file system for backup of physical files in the BB archive.


## Commands

**sync**

Start the sync daemon

**index**

Heavy operation to ensure our SHA-1 FS is consistent with itself.  

**reshape** _folder_ _mode_

Import a local folder with whatever structure to our SHA-1 based folder structure.

_folder_: 
Source folder to import from.

_mode_: "rename" will move files from source folder. Existing and new alike.  

_mode_: "link" will only hard link to source folder.

**version**

Print version and exit

## Contributing

```shell
$ git clone https://github.com/Bnei-Baruch/mdb-fs.git
$ cd mdb-fs
$ make build
```

To work with a local mdb copy

```shell
docker-compose up -d
wget https://kabbalahmedia.info/assets/mdb_dump.sql.gz
gunzip mdb_dump.sql.gz
psql -h localhost -U user -d mdb -p 5434 < mdb_dump.sql
```

## License

MIT