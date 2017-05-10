SARCH - Simple ARCHiving 
=========================================

It's kind of like rsync with database or git annex for dummies.

.. class:: no-web

    .. image:: https://travis-ci.org/susundberg/sarch.svg?branch=master
        :alt: Build status
        :width: 100%
        :align: right



Features:
-----------
* Distributed system for storing (binary) files, no central repository required (but sure you can use it in centralized manner).
* Not version control -> near zero overhead, but you still get some information of the file (created, deleted, modified at what time)
* Simple command line interface familiar from git/svn.
* One can revert changes as at least one repository has the data left. 
* Standalone, works with python3.5+ - no libraries required
* Currently SSH (=server) and FILE (=usb-disk) remotes supported

Usage:
-----------
* sarch init <repository name>
* sarch add <filenames/paths>
* sarch rm <filenames/paths>
* sarch status - fast check whats going one (based on file modtime)
* sarch verify - check md5 of every file for corruption.
* sarch commit - commit changes (--auto to automatically add modified/removed files)
* sarch help - to list available commands
* sarch sync <target>
* sarch log <filenames> - show log of given file

Requirements:
-----------
Python with typing is required, i have tested it with python 3.5 that ships with ubuntu 16.04. Automated testing is runned on python 3.6 and python 3.7 on `Travis <https://travis-ci.org/susundberg/sarch/builds#>`_.




Major features missing:
----------
* Filesystem locking (to prevent several instances)

Minor todo:
----------
* status command should respect the folder where called
* Untracked files should be listed as untracked directory - not all as single files
* When transferring files we could also check for possibility of transferring single file and copy that (== when target has copies of files)
* Should we be able to name the remotes? "sync origin"
* Log on file and conflict resolving should be able to track filename changes due moves
* Proper dummy target (http-set/http-get) support

