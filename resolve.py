#!/usr/bin/env python
#
# File: statwalker_resolve.py
# Author: Santiago Ganis
#
# Description: 
# This program parses the output of statwalker.py and resolves
# unix times, uid, gid, and mode, into
# human times, user, group, file type, and permissions in octal,
# also converts size and disk usage into GB
#
# Version: 1.0
# Date: 04/02/2014
#
import csv
import datetime
import grp
import pwd
import stat
import sys
import time

HEADER = "INODE,ACCESSED,MODIFIED,USER,GROUP,TYPE,PERM,SIZE,DISK,PATH"

USERS = {}


def get_username(uid, path=None):
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
        user = "UNKNOWN"
    USERS[uid] = user
    return user


GROUPS = {}


def get_groupname(gid, path=None):
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
        group = "UNKNOWN"
    GROUPS[gid] = group
    return group


TIMES = {}


def format_time(unix):
    if unix in TIMES:
        return TIMES[unix]
    if unix < 0:
        return datetime.datetime(1, 1, 1)
    # t = datetime.datetime.fromtimestamp(unix).strftime('%Y-%m-%d %H:%M:%S')
    t = datetime.datetime.fromtimestamp(unix).strftime('%Y-%m-%d')
    TIMES[unix] = t
    return t


TYPES = {}


def get_filetype(mode):
    """linux has 7 file types: http://en.wikipedia.org/wiki/Unix_file_types
    """
    if mode in TYPES:
        return TYPES[mode]
    if stat.S_ISREG(mode) != 0:
        typ = 'FILE'
    elif stat.S_ISDIR(mode) != 0:
        typ = 'DIR'
    elif stat.S_ISLNK(mode) != 0:
        typ = 'LINK'
    elif stat.S_ISSOCK(mode) != 0:
        typ = 'SOCK'
    elif stat.S_ISFIFO(mode) != 0:
        typ = 'PIPE'
    elif stat.S_ISBLK(mode) != 0:
        typ = 'BDEV'
    elif stat.S_ISCHR(mode) != 0:
        typ = 'CDEV'
    else:
        typ = 'UNKNOWN'
    TYPES[mode] = typ
    return typ


PERMS = {}


def get_permissions(mode):
    """get permissions from file mode
    """
    # return str(int(oct(stat.S_IMODE(mode)))).zfill(3) # mode & 07777
    return oct(stat.S_IMODE(mode))[-3:]


def run(input, output):
    init = time.time()
    print("Resolving file {}".format(input))
    inodes = {}
    with open(input) as f, open(output, 'w') as w:
        reader = csv.reader(f)
        next(reader, None)
        w.write(HEADER + '\n')
        for r in reader:
            if not r: continue
            try:
                inode = r[0]
                accessed = format_time(int(r[1]))
                modified = format_time(int(r[2]))
                user = get_username(int(r[3]))
                group = get_groupname(int(r[4]))
                permissions = get_permissions(int(r[5]))
                filetype = get_filetype(int(r[5]))
                size = int(r[6]) / 1073741824.0  # GB
                disk = int(r[7]) / 1073741824.0  # GB
                path = r[8]
                w.write("{},{},{},{},{},{},{},{},{},\"{}\"\n".format(
                    inode, accessed, modified, user, group, filetype, permissions, size, disk, path))
            except Exception as ex:
                raise ex
    print("Total resolve time: {} sec.".format(time.time() - init))


if __name__ == '__main__':
    assert len(sys.argv) > 1  # usage: prog <csv>
    input = sys.argv[1]
    run(input, input + ".resolved.csv")
