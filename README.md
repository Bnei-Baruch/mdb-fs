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
$ cd $GOPATH/src/github.com/Bnei-Baruch
$ git clone https://github.com/Bnei-Baruch/mdb-fs.git
$ cd mdb-fs
$ dep ensure
```

## License

MIT