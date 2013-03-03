#!/usr/bin/env python

import os, errno, sys, threading, collections, uuid, shutil, traceback
from fuse import FUSE, FuseOSError, Operations, LoggingMixIn
import libtorrent as lt
import hgapi as hg

class TmpFile(object):
  def __init__(self, path):
    self.path = path

class DPDFS(LoggingMixIn, Operations):
  def __init__(self, name, root):
    self.name = name
    self.root = os.path.realpath(root)
    self.hgdb = os.path.join(self.root, 'hgdb')
    self.tmp = os.path.join(self.root, 'tmp')
    self.dat = os.path.join(self.root, 'dat')
    self.shadow = os.path.join(self.root, 'shadow')
    
    if not os.path.isdir(self.hgdb): os.makedirs(self.hgdb)
    self.repo = hg.Repo(self.hgdb)
    vfn = os.path.join(self.root, '__volume__')
    if os.path.isfile(vfn):
      with open(vfn,'r') as f:
        existing_name = f.read().strip()
        if self.name != existing_name:
          raise Exception('volume name "%s" != existing volume name "%s"' % (self.name, existing_name))
    else:
      with open(vfn,'w') as f:
        f.write(self.name)
      self.repo.hg_init()
    
    if not os.path.isdir(self.tmp): os.makedirs(self.tmp)
    if not os.path.isdir(self.dat): os.makedirs(self.dat)
    if not os.path.isdir(self.shadow): os.makedirs(self.shadow)
    self.rwlock = threading.Lock()
    self.open_files = {}
    print 'init', self.hgdb

  def __call__(self, op, path, *args):
    print op, path, ('...data...' if op=='write' else args)
    if path.startswith('/.Trash'): raise FuseOSError(errno.EACCES)
    if path.endswith('/.__dpdfs_dir__'): raise FuseOSError(errno.EACCES)
    return super(DPDFS, self).__call__(op, path, *args)

  def access(self, path, mode):
    if not os.access(self.hgdb+path, mode):
      raise FuseOSError(errno.EACCES)

#  chmod = os.chmod
#  chown = os.chown

  def create(self, path, mode):
    with self.rwlock:
      tmp = uuid.uuid4().hex
      self.open_files[path] = tmp
      with open(self.hgdb+path,'wb') as f:
        pass
      self.repo.hg_add(self.hgdb+path)
      return os.open(os.path.join(self.tmp, tmp), os.O_WRONLY | os.O_CREAT, mode)

  def flush(self, path, fh):
    with self.rwlock:
      return os.fsync(fh)

  def fsync(self, path, datasync, fh):
    with self.rwlock:
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
    with self.rwlock:
      return os.link(source, target)

  listxattr = None
#  mknod = os.mknod

  def mkdir(self, path, flags):
    with self.rwlock:
      fn = self.hgdb+path
      ret = os.mkdir(fn, flags)
      with open(fn+'/.__dpdfs_dir__','w') as f:
        f.write("hg doesn't track empty dirs, so we add this file...")
      self.repo.hg_add(fn+'/.__dpdfs_dir__')
      self.repo.hg_commit('mkdir %s' % path, files=[fn+'/.__dpdfs_dir__'])
      return ret

  def read(self, path, size, offset, fh):
    with self.rwlock:
      os.lseek(fh, offset, 0)
      return os.read(fh, size)

  def open(self, path, flags):
    with self.rwlock:
      fn = self.hgdb+path
      if not (flags & (os.O_WRONLY | os.O_RDWR | os.O_APPEND | os.O_CREAT | os.O_EXCL | os.O_TRUNC)):
        #print '\treadonly'
        t = get_torrent_dict(fn)
        if t:
          name = t['info']['name']
          return os.open(os.path.join(self.dat, name[:2], name), flags)
        else:
          return os.open(fn, flags)
      tmp = uuid.uuid4().hex
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
    with self.rwlock:
      return ['.', '..'] + [x for x in os.listdir(self.hgdb+path) if x!=".__dpdfs_dir__" and x!='.hg']

#    readlink = os.readlink

  def release(self, path, fh):
    with self.rwlock:
      ret = os.close(fh)
      if path in self.open_files:
        self.finalize(path, self.open_files[path])
        del self.open_files[path]
      return ret
    
  def finalize(self, path, uid):
    #print 'finalize', path, uid
    try:
      fs = lt.file_storage()
      tmp_fn = os.path.join(self.tmp, uid)
      try: st_size = os.stat(tmp_fn).st_size
      except:
        traceback.print_exc()
        return
        
      #print tmp_fn, st_size
      lt.add_files(fs, tmp_fn, st_size)
      t = lt.create_torrent(fs)
      #t.set_creator("dpdfs");
      lt.set_piece_hashes(t, self.tmp)
      tdata = t.generate()
      #print tdata
      with open(self.hgdb+path, 'wb') as f:
        f.write(lt.bencode(tdata))
      #print 'wrote', self.hgdb+path
      dat_dir = os.path.join(self.dat, uid[:2])
      if not os.path.isdir(dat_dir): 
        try: os.mkdir(dat_dir)
        except: pass
      os.rename(tmp_fn, os.path.join(dat_dir, uid))
      if os.path.exists(self.shadow+path): os.remove(self.shadow+path)
      os.symlink(os.path.join(dat_dir, uid), self.shadow+path)
      #print 'committing', self.hgdb+path
      self.repo.hg_commit('wrote %s' % path, files=[self.hgdb+path])
    except Exception as e:
      traceback.print_exc()
      raise e
    
  def rename(self, old, new):
    with self.rwlock:
      self.repo.hg_rename(self.hgdb+old, self.hgdb+new)
      self.repo.hg_commit('rename %s to %s' % (old,new), files=[self.hgdb+old, self.hgdb+new])

#  rmdir = os.rmdir

  def rmdir(self, path):
    with self.rwlock:
      self.repo.hg_remove(self.hgdb+path+'/.__dpdfs_dir__')
      self.repo.hg_commit('rmdir %s' % path, files=[self.hgdb+path+'/.__dpdfs_dir__'])
      #return os.rmdir(self.hgdb+path)

  def statfs(self, path):
        stv = os.statvfs(self.hgdb+path)
        return dict((key, getattr(stv, key)) for key in ('f_bavail', 'f_bfree',
            'f_blocks', 'f_bsize', 'f_favail', 'f_ffree', 'f_files', 'f_flag',
            'f_frsize', 'f_namemax'))

  def symlink(self, target, source):
    with self.rwlock:
      ret = os.symlink(source, target)
      return ret
        

  def truncate(self, path, length, fh=None):
    with self.rwlock:
      with open(os.path.join(self.tmp, self.open_files[path]), 'r+') as f:
        f.truncate(length)

#  utimens = os.utime

  def unlink(self, path):
    with self.rwlock:
      self.repo.hg_remove(self.hgdb+path)
      self.repo.hg_commit('unlink %s' % path, files=[self.hgdb+path])

  def write(self, path, data, offset, fh):
    with self.rwlock:
      os.lseek(fh, offset, 0)
      return os.write(fh, data)


def get_torrent_dict(fn):
  if not os.path.isfile(fn): return
  with open(fn, 'rb') as f:
    return lt.bdecode(f.read())


if __name__ == '__main__':
  if len(sys.argv) != 4:
    print('usage: %s <name> <root> <mountpoint>' % sys.argv[0])
    sys.exit(1)

  fuse = FUSE(DPDFS(sys.argv[1], sys.argv[2]), sys.argv[3], foreground=True)

