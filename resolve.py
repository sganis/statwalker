#!/usr/bin/env python3
#
# File: resolve.py
# Author: Santiago Ganis
#
# Description: 
# This program parses the output of statwalker.py and resolves
# unix times, uid, gid, and mode, into
# human times, user, group, file type, and permissions in octal,
# also converts size and disk usage into GB
#
# Changes
# 04/02/2014, first release
# 08/15/2025, folder aggregation


import os
import sys
import time
import datetime
import csv
import stat
import pwd
import grp

HEADER = "INODE,ACCESSED,MODIFIED,USER,GROUP,TYPE,PERM,SIZE,DISK,PATH"
	
USERS = {}
def get_username(uid,path=None):
	"""username resolution, with caching
	"""
	if uid in USERS:
		return USERS[uid]
	try:
		user = pwd.getpwuid(uid).pw_name
		# cmd = 'stat -c %U "'+path+'"'
		# user = os.popen(cmd).read().strip()
		# user = subprocess.check_output(cmd, shell=True).strip()
	except:	
		user="UNKNOWN"
	USERS[uid] = user		
	return user

GROUPS = {}
def get_groupname(gid,path=None):
	"""groupname resolution, with caching
	"""
	if gid in GROUPS:
		return GROUPS[gid]
	try:
		group = grp.getgrgid(gid).gr_name
		# cmd = 'stat -c %G "'+path+'"'
		# group = os.popen(cmd).read().strip()
		# group = subprocess.check_output(cmd, shell=True).strip()
	except:
		group="UNKNOWN"
	GROUPS[gid] = group
	return group

TIMES={}
def format_time(unix):
	if unix in TIMES:
		return TIMES[unix]	
	if unix<0:
		return datetime.datetime(1,1,1)
	#t = datetime.datetime.fromtimestamp(unix).strftime('%Y-%m-%d %H:%M:%S')
	t = datetime.datetime.fromtimestamp(unix).strftime('%Y-%m-%d')
	TIMES[unix]=t
	return t

TYPES={}
def get_filetype(mode):
	"""linux has 7 file types: http://en.wikipedia.org/wiki/Unix_file_types
	"""
	if mode in TYPES:
		return TYPES[mode]
	if stat.S_ISREG(mode) != 0: 	typ='FILE'
	elif stat.S_ISDIR(mode) != 0: 	typ='DIR'
	elif stat.S_ISLNK(mode) != 0: 	typ='LINK'
	elif stat.S_ISSOCK(mode) != 0: 	typ='SOCK'
	elif stat.S_ISFIFO(mode) != 0: 	typ='PIPE'
	elif stat.S_ISBLK(mode) != 0: 	typ='BDEV'
	elif stat.S_ISCHR(mode) != 0: 	typ='CDEV'
	else: 							typ='UNKNOWN'	
	TYPES[mode]=typ
	return typ

PERMS={}
def get_permissions(mode):
	"""get permissions from file mode
	"""
	return int(format(stat.S_IMODE(mode), 'o'))


AGG = {}
def aggregate(path, disk, mtime, user):
	folder = os.path.dirname(path)
	parts = folder.split('/')
	key = ''

	for p in parts:
		if not p: continue
		key = f'{key}/{p}'

		if key not in AGG:
			AGG[key] = [0,0,0,{}]
		if user not in AGG[key][3]:
		 	AGG[key][3][user] = 1

		AGG[key][0] += 1
		AGG[key][1] += disk
		AGG[key][2] = max(AGG[key][2], mtime)

INODES = {}
def run(input, output):
	init = time.time()
	print("Resolving file %s" % input)
	inodes = {}
	
	with open(input) as f, open(output,'w') as w:
		reader = csv.reader(f)
		next(reader, None)
		w.write(HEADER+'\n')
	
		for r in reader:
			if not r: continue
			# try:
			inode = r[0]
			accessed = format_time(int(r[1]))
			mtime = int(r[2])
			modified = format_time(mtime)
			user = get_username(int(r[3]))
			group = get_groupname(int(r[4]))
			permissions = get_permissions(int(r[5]))
			filetype = get_filetype(int(r[5]))
			size = int(r[6])
			disk = int(r[7])
			path = r[8]

			if inode in INODES:
				disk = 0
			else:
				INODES[inode] = 1
			
			aggregate(path, disk, mtime, user)
			
			w.write("%s,%s,%s,%s,%s,%s,%s,%s,%s,\"%s\"\n" % (
				inode,accessed,modified,user,group,filetype,permissions,size,disk,path))
			# except Exception as ex:
				# print(ex)
			# break
	
	with open(input + '.agg.csv','w') as w:
		for path,v in AGG.items():
			w.write('"%s",%s,%s,%s,%s\n' % (path, v[0], v[1], format_time(v[2]), '|'.join(v[3])))

	print("Total resolve time: %s sec." % (time.time()-init))


if __name__ == '__main__':
	assert len(sys.argv)>1 # usage: prog <csv>
	input = sys.argv[1]
	run(input, input + ".res.csv")

	

