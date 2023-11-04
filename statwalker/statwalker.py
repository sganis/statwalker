#!/usr/bin/env python
#
# File: stat_walker.py
# Author: Santiago Ganis
#
# Description:
#
# Walk PATH recursively and get stats (all info from stat system call).
#
# It has several optimization features:
# - Uses the os.lstat module, which is super fast.
# - Symbolic links are not followed.
# - Run in parallel with the max number of processes in the machine (cores)
#   it can be overriten with -n [processes]
#
# Writes the output in a csv file, default name is 'stats.csv' in the
# current directory. The csv format is the following:
# "INODE,ATIME,MTIME,UID,GID,MODE,SIZE,DISK,PATH"
#
# The -h parameter produces this output:
# usage: %s [-h] [-b BALANCE] [-n NP] [-o OUTPUT] [-s PATTERN] PATH
# positional arguments:
#   PATH	  path to walk and get stats
# optional arguments:
#   -h, --help
#			 show this help message and exit
#   -c, --color
#			 use colors (default: False)
#   -b, --balance BALANCE
#			 balance workload in task assignment (default: 1)
#   -n, --processes NP
#			 number of processes to run in parallel (default: MAX)
#   -o, --output OUTPUT
#			 csv file to write stats (default: full-path-to-folder.csv)
#   --skip PATTERN
#			 skip file name pattern list, separated by comma
#			 (default: None)
#   --sort	sort results (default: False )
#
# Version: 21.0
#
# Date: 07/26/2013
#
# Changelog:
# - 07/27/2013, added parallel version, multiprocess
# - 07/28/2013, added caching
# - 07/29/2013, added balanced task assignment
# - 07/30/2013, added colors
# - 08/10/2013, added skip option
 
import os
import sys
import time
import datetime
import stat
import random
from multiprocessing import Pool, freeze_support
import subprocess

is_windows = os.name == 'nt'

if is_windows:
	import getsid

HEADER = "INODE,ATIME,MTIME,UID,GID,MODE,SIZE,DISK,PATH"
PATH=""
OUTPUT=""
COLOR=True


def listdir(path):
	l = []
	try:
		l = os.listdir(path)
	except Exception as ex:
		print('Cannot list dir {}: {}'.format(path, ex))
	return l


def get_stats(path):
	"""cvs line, see HEADER
	returns tuple (is_dir, line, disk)
	HEADER = "INODE,ATIME,MTIME,UID,GID,SIZE,DISK,PATH"
	"""
	f = ()
	blocks = 0
	uid = 0
	try:
		f = os.lstat(path)
		# python stat struct:
		# st_mode,st_ino,st_dev,st_nlink,st_uid,st_gid,st_size,st_atime,st_mtime,st_ctime
		# return DEV,INODE,ATIME,MTIME,UID,GID,MODE,SIZE,BLOKKS
		if is_windows:
			uid = getsid.get_sid(path)
		else:
			uid = f.st_uid
			blocks = f.st_blocks
	except Exception as ex:
		print('Cannot get stats {}: {}'.format(path, ex))

	if not f:
		return False,'',0
	# path = path.encode('utf-8', errors='surrogatepass')
	mode = f[0]
	disk = blocks*512
	size = f[6]
	# INODE,ATIME,MTIME,UID,GID,MODE,SIZE,DISK,PATH"
	line = '%s-%s,%s,%s,%s,%s,%s,%s,%s,%s' % (
			f[2],f[1],f[7],f[8],uid,f[5],f[0],f[6],disk,'"' + path +'"')
	# try:
	# 	print(line)
	# except Exception as ex:
	# 	for i,e in enumerate(f):
	# 		print(i, e)
	# 	print(f)
	# 	print(path.encode('utf-8', errors='surrogatepass'))
	return stat.S_ISDIR(mode), line, size
 
 
SKIP=[]
def skipit(path):
	"""skip file if in patter
	"""
	if SKIP:
		for s in SKIP:
			if s in path:
				return True
	return False
 
def walk(path, output):
	"""recursively descend the directory tree rooted at path,
	and collect stats for each file, links are not followed
	Params:
	path - folder to walk recursively
	"""
	total_count=0
	total_size=0
	is_dir, line, size = get_stats(path)
	if line:
		try:
			output.write('{}\n'.format(line))
		except Exception as ex:
			print('Cannot write file: {}. {}'.format(repr(path), ex))
		total_count=1
	if not is_dir:
		return total_count, size	
	dirs = listdir(path)		
	files = [r'%s' % os.path.join(path,f) for f in dirs]
	for path in files:
		if skipit(path): continue
		count, size = walk(path, output)
		total_count += count
		total_size += size
	return total_count, total_size

def get_chunks(seq, num):
	"""split seq in chunks of size num,
	used to divide tasks for workers
	"""
	avg = len(seq)/float(num)
	out = []
	last = 0.0
	while last < len(seq):
		out.append(seq[int(last):int(last + avg)])
		last += avg
	return out
 
def worker(chunk):
	"""Function to run in parallel
	chunk is a list of directories or files
	creates a file with the process id appended to the filename
	return a tuple with pid, time, chunk, and a list of count/size 
	to print folders with max file count and size
	"""
	start = time.time()
	count_size = []
	pid = os.getpid()
	if not 'TEMP' in os.environ:
		return pid,0,0,0
	temp = '{}.{}'.format(os.environ['TEMP'], pid)
	with open(temp,'w', encoding="utf8") as w:
		for f in chunk:
			count, size = walk(f, w)
			count_size.append([f, count, size])	
	seconds = time.time()-start
	return (pid, seconds, chunk, count_size)
 
def report_parallel(procs, total_time):
	"""procs is a tuple (prid, time_spent, chunk, [count_size])
	"""
	percents = []
	max_files = 0	 # folder with max file count
	max_bytes = 0 # folder with max size
	folder_max_count = ""
	folder_max_bytes = ""
	total_files = 0
	for p in procs:
		percent = p[1]/float(total_time)
		percents.append(percent)
		bar = ('=' * int(percent * 10)).ljust(10)
		perc = "%.2f" % round(percent*100,2)
		files_chunk=0
		for c in p[3]:
			total_files += c[1]
			files_chunk += c[1]
			if c[1] > max_files:
				max_files = c[1]
				folder_max_count = c[0]
			if c[2] > max_bytes:
				max_bytes = c[2]
				folder_max_bytes = c[0]
		print("PID: %s\t\t%s sec [%s] %s%% [%s files]" % (p[0], round(p[1],1), bar,perc.rjust(5),files_chunk))

		#pprint.pprint(p[2])
	times = [p[1] for p in procs]
	files = [p[3] for p in procs]
	avg = sum(times)/float(len(times))
	diff = max(percents)-min(percents)
	print("Total files by workers: %s" % (total_files))
	print("Folder with max files: \t%s [%s files]" % (folder_max_count, max_files))
	print("Folder with max size: \t%s [%s]" % (folder_max_bytes, bytes_human(max_bytes)))
	print("Avg time by workers: \t%s sec" % round(avg,2))
	print("Difference (Max-Min): \t%s%%" % round(diff*100,2))
	if diff > 0.3:
		print("Task division unbalanced. Use the -b parameter to fix it.")
	else:
		print("Work balance Ok.")
	return total_files
 
def get_directories(path, level=1):
	dirs = []
	rest=[]
	dirs = listdir(path)
	fulldirs = [r'%s' % os.path.join(path,f) for f in dirs]
	files=[]
	for f in fulldirs:
		if skipit(f): continue   
		is_dir, line, disk = get_stats(f)	
		# this only works with python
		# if os.isdir(f) and os.islink(f) and level>0:	
		if is_dir and level>0:
			level-= 1
			d, r = get_directories(f, level)
			files.extend(d)		 
			rest.append(line)
			rest.extend(r)
			level+=1
		else:
			files.append(f)
	return files, rest
			   
def bytes_human(num):
	for x in ['bytes','KB','MB','GB']:
		if num < 1024.0:
			return "%3.1f%s" % (num, x)
		num /= 1024.0
	return "%3.1f%s" % (num, 'TB')
		
# def stat_root(root, output):
# 	dirs = {}
# 	for r in root:
# 		path = "/"	
# 		for p in r.split('/'):
# 			if not p: continue
# 			path += p
# 			if path not in dirs:
# 				dirs[path]=path
# 				is_dir, line, bytes = get_stats(path)
# 				output.write(line+'\n')
# 			path += '/'

def get_param():
	"""handle command line parameters, test using -h parameter...
	"""
	import argparse
	parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
	parser.add_argument('PATH', help='path to walk and get stats')
	parser.add_argument('-b', '--balance', default=3, help='balance workload in task assignment')
	parser.add_argument('-c', '--color', action='store_false', default=True, help='cancel colors')
	parser.add_argument('-n', '--processes', default='MAX', help='number of processes to run in parallel')
	parser.add_argument('-o', '--output', help='csv file to write stats')
	parser.add_argument('--skip', default=None, help='skip file name pattern list, separated by comma')
	parser.add_argument('--sort', action='store_true', default=False, help='sort results')
	args = parser.parse_args()
	PATH = args.PATH
	BALANCE = args.balance
	COLOR = args.color
	NP = args.processes
	OUTPUT = args.output
	SKIP = args.skip
	SORT = args.sort
	return (PATH,BALANCE,COLOR,NP,OUTPUT,SKIP,SORT)
 
 
def main():
	# parse parameters
	global COLOR,SKIP
	PATH,BALANCE,COLOR,NP,OUTPUT,SKIP,SORT = get_param()			
	PATH = [os.path.abspath(p) for p in PATH.split(',')]
	if not OUTPUT:
		if is_windows:
			OUTPUT = os.path.realpath(PATH[0]).replace('\\','-').replace(':','')[2:] + '.csv'
		else:
			OUTPUT = os.path.realpath(PATH[0]).replace('/','-')[1:] + '.csv'
	OUTPUT = os.path.realpath(OUTPUT)
	if is_windows:
		assert 'TEMP' in os.environ
	else:
		os.environ['TEMP'] = OUTPUT

	BALANCE = int(BALANCE)
	if NP != 'MAX': NP = int(NP)	   
	if SKIP: SKIP=SKIP.split(',')
	assert BALANCE in range(1,10)
	
	# don't overwrite files  
	# if os.path.isdir(OUTPUT):
	# 	print 'OUTPUT is a folder: %s' % OUTPUT
	#	 sys.exit()			
	# if not os.access(os.path.dirname(OUTPUT), os.W_OK):
	#	 print 'No write access to %s' % os.path.dirname(OUTPUT)
	#	 sys.exit()
	# if os.path.exists(OUTPUT):
	#	 resp = raw_input(red('Overwrite %s ? [y/n]: ' % OUTPUT))
	#		 if resp is not "y":
	#	 		sys.exit()
	
	# start  
	print("/*************** stat_walker.py *************************************/")
	print("Command: %s" % " ".join(sys.argv[:]))
	for p in PATH:
		print("Input:  %s" % p)
	print("Output: %s" % OUTPUT)
	print("Balance: %s" % BALANCE)
	if SORT: 	print("Sort: %s" % SORT)
	if SKIP:	print("Skip: %s" % SKIP)
	
	start = 0
	end = 0
	total = 0			   
	total_seconds = 0
	lines = []

	if NP=='MAX':	
		pool = Pool()
	else:			  
		pool = Pool(int(NP))
	N = len(pool._pool)
	print("Running with %s processes..." % N)
 
	# serial preprocessing
	start = time.time()
	# devide the task, shuffle to make it BALANCE
	#dirs = [os.path.join(PATH,f) for f in os.listdir(PATH)]
	dirs = []
	rest = []
	for p in PATH:
		d, r = get_directories(p, BALANCE)
		dirs.extend(d)
		rest.extend(r)
	# import pprint
	# pprint.pprint(dirs)
	# pprint.pprint(rest)
	# print "Initial folders:", len(dirs),
	random.shuffle(dirs)
	chunks = list(get_chunks(dirs, N))
	# pprint.pprint(chunks)
	# print "chunks: %s, dirs: %s" % (len(chunks),sum([len(x) for x in chunks]))
	# print "Rest of directories without stats: %s" % len(rest)
	# pprint.pprint(rest)
	seconds_pre = time.time()-start
 
	# run workers in parallel, wait until all workers finish
	start = time.time()
	procs = pool.map_async(worker, chunks).get(999999)
	seconds_parallel = time.time()-start
			   
	# serial postprocessing
	start = time.time()			  
	# write output
	w = open(OUTPUT, 'w', encoding="utf8")
	# csv header
	header = HEADER
	w.write(header + '\n')
			   
	# stat root, we need this for effective permissions post-processing tool
	# stat_root(PATH, w)
 
	# stat dir, and merge result from workers
	post_total = 0
	for line in rest:
		w.write(line+'\n')
		post_total+=1
	for p in procs:
		fname = '%s.%s' % (os.environ['TEMP'], p[0])
		# fname = OUTPUT +'.'+str(p[0])
		if not os.path.exists(fname): continue
		infile = open(fname, encoding='utf-8')
		for line in infile:
			w.write(line)
			total+=1
		infile.close()
		os.remove(fname)
	w.close()

	end = time.time()
	seconds_post = end-start
	total_seconds = seconds_pre + seconds_parallel + seconds_post

	print("Pre-process:  \t\t%s sec" % (round(seconds_pre,2)))
	worker_total  = report_parallel(procs, seconds_parallel)
	print("Post-process: \t\t%s sec [%s files]" % (round(seconds_post,2), post_total))
	total += post_total
	# change permission in output
	os.chmod(OUTPUT,0o666)

	# finish, show some summary
	report = "Total files: \t\t%s\n" % (total)
	report += "Total time spent: \t%s sec " % round(total_seconds,2)
	report += "[%s]\n" % datetime.timedelta(seconds=total_seconds)
	report += "Rate: \t\t\t%s files/sec\n" % int(total/total_seconds)
	report += "Output: %s [%s]\n" % (OUTPUT, bytes_human(os.lstat(OUTPUT).st_size))
	report += "/*******************************************************************/"
	print(report)

	# sort results to compare easily
	if SORT:
		import stat_sort
		stat_sort.sort(OUTPUT, has_header=True, inplace=True)
	print("Done.\n")
	

if __name__ == '__main__':
	freeze_support()
	main()