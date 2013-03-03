#!/usr/bin/env python

import os, errno, sys, threading, collections, uuid, shutil
from fuse import FUSE, FuseOSError, Operations, LoggingMixIn
import libtorrent as lt

class TmpFile(object):
  def __init__(self, path):
    self.path = path

class DPDFS(LoggingMixIn, Operations):
  def __init__(self, root):
    self.root = os.path.realpath(root)
    self.hgdb = os.path.join(self.root, 'hgdb')
    self.tmp = os.path.join(self.root, 'tmp')
    self.dat = os.path.join(self.root, 'dat')
    self.shadow = os.path.join(self.root, 'shadow')
    if not os.path.isdir(self.hgdb): os.makedirs(self.hgdb)
    if not os.path.isdir(self.tmp): os.makedirs(self.tmp)
    if not os.path.isdir(self.dat): os.makedirs(self.dat)
    if not os.path.isdir(self.shadow): os.makedirs(self.shadow)
    self.rwlock = threading.Lock()
    self.open_files = {}
    print 'init', self.hgdb

  def __call__(self, op, path, *args):
    print op, path, ('...data...' if op=='write' else args)
    return super(DPDFS, self).__call__(op, path, *args)

  def access(self, path, mode):
    if not os.access(self.hgdb+path, mode):
      raise FuseOSError(errno.EACCES)

#  chmod = os.chmod
#  chown = os.chown

  def create(self, path, mode):
    tmp = uuid.uuid4().hex
    self.open_files[path] = tmp
    return os.open(os.path.join(self.tmp, tmp), os.O_WRONLY | os.O_CREAT, mode)

  def flush(self, path, fh):
        return os.fsync(fh)

  def fsync(self, path, datasync, fh):
        return os.fsync(fh)

  def getattr(self, path, fh=None):
    st_size = None
    if path in self.open_files:
      fn = os.path.join(self.tmp, self.open_files[path])
    else:
      fn = self.hgdb+path
      if os.path.isfile(fn):
        with open(fn, 'rb') as f:
          st_size = lt.bdecode(f.read())['info']['length']
    st = os.lstat(fn)
    ret = dict((key, getattr(st, key)) for key in ('st_atime', 'st_ctime',
            'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size', 'st_uid'))
    if st_size is not None:
      ret['st_size'] = st_size
    return ret

  getxattr = None
  
  def link(self, target, source):
        return os.link(source, target)

  listxattr = None
#  mknod = os.mknod

  def mkdir(self, path, flags):
    return os.mkdir(self.hgdb+path, flags)

  def read(self, path, size, offset, fh):
    with self.rwlock:
      os.lseek(fh, offset, 0)
      return os.read(fh, size)

  def open(self, path, flags):
    if not (flags & (os.O_WRONLY | os.O_RDWR | os.O_APPEND | os.O_CREAT | os.O_EXCL | os.O_TRUNC)):
      print '\treadonly'
      t = get_torrent_dict(self.hgdb+path)
      if t:
        name = t['info']['name']
        return os.open(os.path.join(self.dat, name[:2], name), flags)
      else:
        return os.open(self.hgdb+path, flags)
    tmp = uuid.uuid4().hex
    fn = self.hgdb+path
    if os.path.isfile(fn):
      with open(fn, 'rb') as f:
        prev = lt.bdecode(f.read())['info']['name']
        prev_fn = os.path.join(self.dat, prev[:2], prev)
        if os.path.isfile(prev_fn):
          shutil.copyfile(prev_fn, os.path.join(self.tmp, tmp))
    self.open_files[path] = tmp
    return os.open(os.path.join(self.tmp, tmp), flags)
    return 0
  
    
  def readdir(self, path, fh):
    return ['.', '..'] + os.listdir(self.hgdb+path)

#    readlink = os.readlink

  def release(self, path, fh):
    ret = os.close(fh)
    if path in self.open_files:
      self.finalize(path, self.open_files[path])
      del self.open_files[path]
    return ret
    
  def finalize(self, path, uid):
    print 'finalize', path, uid
    fs = lt.file_storage()
    tmp_fn = os.path.join(self.tmp, uid)
    st_size = os.stat(tmp_fn).st_size
    print tmp_fn, st_size
    lt.add_files(fs, tmp_fn, st_size)
    t = lt.create_torrent(fs)
    #t.set_creator("dpdfs");
    print self.tmp
    lt.set_piece_hashes(t, self.tmp)
    print t.generate()
    with open(self.hgdb+path, 'wb') as f:
      f.write(lt.bencode(t.generate()))
    
    dat_dir = os.path.join(self.dat, uid[:2])
    os.makedirs(dat_dir)
    os.rename(tmp_fn, os.path.join(dat_dir, uid))
    os.symlink(os.path.join(dat_dir, uid), self.shadow+path)

  def rename(self, old, new):
    return os.rename(self.hgdb+old, self.hgdb+new)

#  rmdir = os.rmdir

  def rmdir(self, path, flags):
    return os.rmdir(self.hgdb+path, flags)

  def statfs(self, path):
        stv = os.statvfs(self.hgdb+path)
        return dict((key, getattr(stv, key)) for key in ('f_bavail', 'f_bfree',
            'f_blocks', 'f_bsize', 'f_favail', 'f_ffree', 'f_files', 'f_flag',
            'f_frsize', 'f_namemax'))

  def symlink(self, target, source):
        return os.symlink(source, target)

  def truncate(self, path, length, fh=None):
    with open(os.path.join(self.tmp, self.open_files[path]), 'r+') as f:
            f.truncate(length)

#  utimens = os.utime

  def unlink(self, path):
    return os.unlink(self.hgdb+path)

  def write(self, path, data, offset, fh):
        with self.rwlock:
            os.lseek(fh, offset, 0)
            return os.write(fh, data)


def get_torrent_dict(fn):
  if not os.path.isfile(fn): return
  with open(fn, 'rb') as f:
    return lt.bdecode(f.read())


if __name__ == '__main__':
  if len(sys.argv) != 3:
    print('usage: %s <root> <mountpoint>' % sys.argv[0])
    sys.exit(1)

  fuse = FUSE(DPDFS(sys.argv[1]), sys.argv[2], foreground=True)

