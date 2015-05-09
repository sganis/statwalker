**********
Statwalker
**********

Walk file systems and collect stats.

.. contents:: 

Summary
=======

Statwalker is a command-line program that scan files recursively (normally called as "walk") and collects stats, basically file names and metadata (inode information in Linux systems). It runs in parallel in a single machine, and the output is a comma-separated file (csv), one line per file. These results can be analysed using other tools (see below).

Output
======

The csv file will look like this:

.. code::
	
	INODE,ATIME,MTIME,UID,GID,MODE,SIZE,DISK,PATH
	2050-27525121,1431077250,1398410037,0,0,16877,4096,4096,"/path"
	2050-27525362,1431159769,1431156403,1000,1000,16877,4096,4096,"/path/folder1"
	2050-27526724,1431154531,1431154525,1000,1000,16877,12288,12288,"/path/folder1/dir1"
	2050-27526728,1431161312,1410607856,1000,1000,16877,4096,4096,"/path/folder1/dir1/file1"
	2050-31982359,1431116813,1418133470,1000,1000,16895,4096,4096,"/path/folder1/dir1/file2"
	2050-48367955,1431116812,1423420614,1000,1000,16893,4096,4096,"/path/folder1/dir1/file3"

Column description:

1. INODE: device identifier and inode (Linux)
2. ATIME: last access time in unix format (seconds since epoc)
3. MTIME: last modified time in unix fromat
4. UID: user ID
5. GID: group ID
6. MODE: mode, which is file type and permissions
7. SIZE: real size in bytes, same value reported with command du -b
8. DISK: disk usage, which is number of blocks times 512
9. PATH: full path


As noticed, the information is not very human readable, for performance reasons. 
A tool is available in the source folder called resolve.py, that I use to
translate that file into a more useful version. This resolved file will look like this 
(this time the columns are self-documented by the name, with size and disk in GB):

.. code::
	
	INODE,ACCESSED,MODIFIED,USER,GROUP,TYPE,PERM,SIZE,DISK,PATH
	2050-27525121,2015-05-08,2014-04-25,root,root,DIR,755,3.81469726562e-06,3.81469726562e-06,"/path"
	2050-27525362,2015-05-09,2015-05-09,user,user,DIR,755,3.81469726562e-06,3.81469726562e-06,"/path/folder1"
	2050-27526724,2015-05-09,2015-05-09,user,user,DIR,755,1.14440917969e-05,1.14440917969e-05,"/path/folder1/dir1"
	2050-27526728,2015-05-09,2014-09-13,user,user,DIR,755,3.81469726562e-06,3.81469726562e-06,"/path/folder1/dir1/file1"
	2050-31982359,2015-05-08,2014-12-09,user,user,DIR,777,3.81469726562e-06,3.81469726562e-06,"/path/folder1/dir1/file2"


How it works
============

Collecting stats is as simple as this one-liner in bash:

.. code:: bash

	$ TODO

There are many tools doing the same thing, the problem is performance. After trying some tools in a file system with many terabytes of data and millions of files, the problem became untractable. I run statwalker in a storage with 100+ millions of files, with a reading rate over NFS folders of 3000 files/second on average, and much faster if disks are local.


Installation
============

Use pip:

.. code:: bash

    $ pip install statwalker


Usage
=====

Run it from the command line:

.. code::
	
	$ statwalker -h

	usage: statwalker.py [-h] [-b BALANCE] [-c] [-n PROCESSES] [-o OUTPUT]
	                     [--skip SKIP] [--sort]
	                     PATH

	positional arguments:
	  PATH                  path to walk and get stats

	optional arguments:
	  -h, --help            show this help message and exit
	  -b BALANCE, --balance BALANCE
	                        balance workload in task assignment
	  -c, --color           cancel colors
	  -n PROCESSES, --processes PROCESSES
	                        number of processes to run in parallel
	  -o OUTPUT, --output OUTPUT
	                        csv file to write stats
	  --skip SKIP           skip file name pattern list, separated by comma
	  --sort                sort results


You can experiment and compare results with different options, for example:

.. code::

	$ statwalker /path -b7
	
	/*************** statwalker.py *************************************/
	Command: statwalker /path -b7
	Input: /path
	Output: /home/user/home-user-apps.csv
	Balance: 7
	Running with 4 processes...
	Pre-process:  		0.15 sec
	PID: 18239		0.2 sec [=======   ] 70.62% [24365 files]
	PID: 18240		0.2 sec [======    ] 63.31% [27524 files]
	PID: 18241		0.2 sec [=======   ] 78.06% [33920 files]
	PID: 18242		0.2 sec [========  ] 82.98% [34471 files]
	Total files by workers: 120280
	Folder with max files: 	/path/folder_with_many_files [3720 files]
	Folder with max size: 	/path/big_file [1.0GB]
	Avg time by workers: 	0.2 sec
	Difference (Max-Min): 	19.66%
	Work balance Ok.
	Post-process: 		0.0 sec [2793 files]
	Total files: 		2793
	Total time spent: 	0.43 sec [0:00:00.427657]
	Rate: 			6530 files/sec
	Output: path.csv [338.6KB]
	/*******************************************************************/
	Done.



Contribute
==========

Clone the github repository:

.. code:: bash

    $ git clone https://github.com/sganis/statwalker.git


TODO
====

Add documentation for analysis tools: resolution, aggregation, plots, benchmark with c++ and mpi versions.

