**********
Dutopia
**********

Walk file systems and collect stats.

.. contents:: 

Summary
=======

Dutopia is a command-line program that scan files recursively (normally called as "walk") and collects stats, basically file names and metadata (inode information in Linux systems). It runs in parallel in a single machine, and the output is a comma-separated file (*.csv), one line per file. These results can be analysed using other tools (see below).

Output
======

The csv file will look like this:

.. code::
	
	INODE,ATIME,MTIME,UID,GID,MODE,SIZE,DISK,PATH

Colum description:

	1. INODE: device identifier and inode (Linux)
	2. ATIME: last access time in unix format (seconds since epoc)
	3. MTIME: last modified time in unix fromat
	4. UID: user ID
	5. GID: group ID
	6. MODE: mode, which is file type and permissions
	7. SIZE: real size in bytes, same value reported with command du -b
	8. DISK: disk usage, which is number of blocks times 512
	9. PATH: full path

How it works
============

Collecting stats is as simple as this one-liner in bash:

.. code:: bash

	$ TODO

There are many tools doing the same thing, the problem is performance. After trying some tools in a file system with many terabytes of data and millions of files, the problem became untractable. I run Dutopia in a storage with 100+ millions of files, with a reading rate over NFS folders of 3000 files/second on average, and much faster if disks are local.


Installation
============

Use pip:

.. code:: bash

    $ pip install Dutopia


Usage
=====

.. code::
	
	# run it from the command line to see available parameters:
	$ Dutopia -h

	# run it with options
	$ Dutopia -o output.csv /home


Contribute
==========

Clone the github repository:

.. code:: bash

    $ git clone https://github.com/sganis/Dutopia.git


TODO
====

* Add documentation with analysis tools, resolution, agregation, benchmark with c++ and mpi versions.
* Add notes for windows users

