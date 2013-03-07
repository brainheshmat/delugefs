#!/usr/bin/env python

import os, errno, sys, threading, collections, uuid, shutil, traceback, random, select, threading, time, socket
from fuse import FUSE, FuseOSError, Operations, LoggingMixIn
import libtorrent as lt
import hgapi as hg
import pybonjour
import jsonrpc

class Peer(object):
  def __init__(self, service_name, host, port):
    self.service_name = service_name
    self.host = host
    self.addr = socket.gethostbyname(host)
    self.port = port
    self.server = jsonrpc.ServerProxy(jsonrpc.JsonRpc20(), jsonrpc.TransportTcpIp(addr=(host,port)))
    self.hg_port = self.server.get_hg_port()
    self.bt_port = self.server.get_bt_port()

class DPDFS(LoggingMixIn, Operations):
  def __init__(self, name, root, create=False):
    self.name = name
    self.root = os.path.realpath(root)
    self.hgdb = os.path.join(self.root, 'hgdb')
    self.tmp = os.path.join(self.root, 'tmp')
    self.dat = os.path.join(self.root, 'dat')
    self.shadow = os.path.join(self.root, 'shadow')
    self.rpc_port = random.randint(10000, 20000)
    self.hg_port = random.randint(10000, 20000)
    self.peers = {}
    self.bt_handles = {}
    self.bt_in_progress = set()
    self.should_push = False
    
    bt_start_port = random.randint(10000, 20000)
    self.bt_session = lt.session()
    self.bt_session.listen_on(bt_start_port, bt_start_port+10)
    self.bt_port = self.bt_session.listen_port()
    self.bt_session.start_lsd()
    self.bt_session.start_dht()
    print 'libtorrent listening on:', self.bt_port
    self.bt_session.add_dht_router('localhost', 10670)
    time.sleep(1)
    print '...is_dht_running()', self.bt_session.dht_state()
    
    t = threading.Thread(target=self.__start_listening_bonjour)
    t.daemon = True
    t.start()
    
    if not os.path.isdir(self.hgdb): os.makedirs(self.hgdb)
    self.repo = hg.Repo(self.hgdb)
    vfn = os.path.join(self.root, '__volume__')
    if os.path.isfile(vfn):
      with open(vfn,'r') as f:
        existing_name = f.read().strip()
        if self.name != existing_name:
          raise Exception('volume name "%s" != existing volume name "%s"' % (self.name, existing_name))
    else:
      if create:
        with open(vfn,'w') as f:
          f.write(self.name)
        self.repo.hg_init()
      else:
        for i in range(30):
          if self.peers:
            print 'found peer!', self.peers
            break
          time.sleep(1)
        if not self.peers:
          raise Exception('--create not specified, no repo exists and no peers found')
        
        try:
          apeer = self.peers[iter(self.peers).next()]
          #server = jsonrpc.ServerProxy(jsonrpc.JsonRpc20(), jsonrpc.TransportTcpIp(addr=addr))
          #remote_hg_port = apeer.server.get_hg_port()
          self.repo.hg_init()
          self.repo.hg_pull('http://%s:%i' % (apeer.host, apeer.hg_port))
          with open(vfn,'w') as f:
            f.write(self.name)
          self.repo.hg_update('tip')
        except Exception as e:
          if os.path.isdir(os.path.join(self.hgdb, '.hg')):
            shutil.rmtree(os.path.join(self.hgdb, '.hg'))
          traceback.print_exc()
          raise e
        print 'success cloning repo!'
        
    
    if not os.path.isdir(self.tmp): os.makedirs(self.tmp)
    for fn in os.listdir(self.tmp): os.remove(os.path.join(self.tmp,fn))
    if not os.path.isdir(self.dat): os.makedirs(self.dat)
    if not os.path.isdir(self.shadow): os.makedirs(self.shadow)
    self.rwlock = threading.Lock()
    self.open_files = {}
    print 'init', self.hgdb
    t = threading.Thread(target=self.__register, args=())
    t.daemon = True
    t.start()

    t = threading.Thread(target=self.__keep_pushing)
    t.daemon = True
    t.start()

    t = threading.Thread(target=self.__load_local_torrents)
    t.daemon = True
    t.start()

    t = threading.Thread(target=self.__monitor)
    t.daemon = True
    t.start()


  def __monitor(self):
    while True:
      #print '='*80
      for path, h in self.bt_handles.iteritems():
        s = h.status()
        #if s.state==5 and s.download_rate==0 and s.upload_rate==0: continue
        state_str = ['queued', 'checking', 'downloading metadata', 'downloading', 'finished', 'seeding', 'allocating']
        print path, 'is %.2f%% complete (down: %.1f kb/s up: %.1f kB/s peers: %d) %s' % \
            (s.progress * 100, s.download_rate / 1000, s.upload_rate / 1000, \
            s.num_peers, "")
      time.sleep(3)
        
        

  
  def __load_local_torrents(self):
    #print 'self.hgdb', self.hgdb
    for root, dirs, files in os.walk(self.hgdb):
      #print 'root, dirs, files', root, dirs, files
      if root.startswith(os.path.join(self.hgdb, '.hg')): continue
      for fn in files:
        if fn=='.__dpdfs_dir__': continue
        fn = os.path.join(root, fn)
        #print 'checking', fn
        e = get_torrent_dict(fn)
        #with open(fn,'rb') as f:
        #  e = lt.bdecode(f.read())
        uid = e['info']['name']
        info = lt.torrent_info(e)
        dat_file = os.path.join(self.dat, uid[:2], uid)
        #print 'dat_file', dat_file
        if os.path.isfile(dat_file):
          if not os.path.isdir(os.path.dirname(dat_file)): os.mkdir(os.path.dirname(dat_file))
          h = self.bt_session.add_torrent({'ti':info, 'save_path':os.path.join(self.dat, uid[:2])})
          #h = self.bt_session.add_torrent(info, os.path.join(self.dat, uid[:2]), storage_mode=lt.storage_mode_t.storage_mode_sparse)
          print 'added ', fn, '(%s)'%uid
          self.bt_handles[fn[len(self.hgdb):]] = h
    print 'self.bt_handles', self.bt_handles


  def __add_torrent_and_wait(self, path, t):
    uid = t['info']['name']
    info = lt.torrent_info(t)
    dat_file = os.path.join(self.dat, uid[:2], uid)
    if not os.path.isdir(os.path.dirname(dat_file)): os.mkdir(os.path.dirname(dat_file))
    h = self.bt_session.add_torrent({'ti':info, 'save_path':os.path.join(self.dat, uid[:2])})
    #h.set_sequential_download(True)
    for peer in self.peers.values():
      print 'adding peer:', (peer.addr, peer.port)
      h.connect_peer(('127.0.0.1', peer.bt_port), 0)
    print 'added ', path
    self.bt_handles[path] = h
    self.bt_in_progress.add(path)
    while not os.path.isfile(dat_file):
      time.sleep(.1)
    print 'file created'

 
  def __keep_pushing(self):
    while True:
      if self.should_push:
        for peer in self.peers.values():
          #self.repo.hg_push('http://%s:%i' % (peer.host, peer.hg_port))
          peer.server.you_should_pull_from(self.bj_name)
        self.should_push = False
      time.sleep(10)

  def __start_listening_bonjour(self):
    browse_sdRef = pybonjour.DNSServiceBrowse(regtype="_dpdfs._tcp", callBack=self.__bonjour_browse_callback)
    try:
      try:
        while True:
          ready = select.select([browse_sdRef], [], [])
          if browse_sdRef in ready[0]:
            pybonjour.DNSServiceProcessResult(browse_sdRef)
      except KeyboardInterrupt:
          pass
    finally:
      browse_sdRef.close()

  def __bonjour_browse_callback(self, sdRef, flags, interfaceIndex, errorCode, serviceName, regtype, replyDomain):
    #print 'browse_callback', sdRef, flags, interfaceIndex, errorCode, serviceName, regtype, replyDomain
    if errorCode != pybonjour.kDNSServiceErr_NoError:
        return
    if not (flags & pybonjour.kDNSServiceFlagsAdd):
        #print 'browse_callback service removed', sdRef, flags, interfaceIndex, errorCode, serviceName, regtype, replyDomain
        if serviceName in self.peers:
          del self.peers[serviceName]
        print 'self.peers', self.peers
        return
    #print 'Service added; resolving'
    resolve_sdRef = pybonjour.DNSServiceResolve(0, interfaceIndex, serviceName, regtype, replyDomain, self.__bonjour_resolve_callback)
    try:
      while not resolved:
        ready = select.select([resolve_sdRef], [], [], 5)
        if resolve_sdRef not in ready[0]:
          #print 'Resolve timed out'
          break
        pybonjour.DNSServiceProcessResult(resolve_sdRef)
      else:
        resolved.pop()
    finally:
      resolve_sdRef.close()

  def __bonjour_resolve_callback(self, sdRef, flags, interfaceIndex, errorCode, fullname, hosttarget, port, txtRecord):
    if errorCode == pybonjour.kDNSServiceErr_NoError:
      if port==self.rpc_port:
        #print 'ignoring my own service'
        return
      if not (fullname.startswith(self.name+'__') and '._dpdfs._tcp.' in fullname):
        #print 'ignoring unrelated service', fullname
        return
      #print 'resolve_callback', sdRef, flags, interfaceIndex, errorCode, fullname, hosttarget, port, txtRecord
      sname = fullname[:fullname.index('.')]
      resolved.append(True)
      self.peers[sname] = Peer(sname, hosttarget, port)
      print 'self.peers', self.peers


  def get_hg_port(self):
    return self.hg_port
    
  def get_bt_port(self):
    return self.bt_port
    
  def you_should_pull_from(self, peer_name):
    if peer_name in self.peers:
      apeer = self.peers[peer_name]
      self.repo.hg_pull('http://%s:%i' % (apeer.host, apeer.hg_port))
      self.repo.hg_update('tip')
      return 'i updated, thanks!'
    else:
      return "i don't know you, "+ peer_name
  
  def __register(self):
    #return

    server = jsonrpc.Server(jsonrpc.JsonRpc20(), jsonrpc.TransportTcpIp(addr=('', self.rpc_port), logfunc=jsonrpc.log_file("myrpc.%i.log"%self.rpc_port)))
    server.register_function(self.get_hg_port)
    server.register_function(self.get_bt_port)
    server.register_function(self.you_should_pull_from)
    t = threading.Thread(target=server.serve)
    t.daemon = True
    t.start()

    t = threading.Thread(target=self.repo.hg_serve, args=(self.hg_port,))
    t.daemon = True
    t.start()
    print 'http://localhost:%i/' % self.hg_port

    
    print 'registering bonjour listener...'
    self.bj_name = self.name+'__'+uuid.uuid4().hex
    bjservice = pybonjour.DNSServiceRegister(name=self.bj_name, regtype="_dpdfs._tcp", 
                                             port=self.rpc_port, callBack=self.__bonjour_register_callback)
    try:
      while True:
        ready = select.select([bjservice], [], [])
        if bjservice in ready[0]:
          pybonjour.DNSServiceProcessResult(bjservice)
    except KeyboardInterrupt:
      pass

  def __bonjour_register_callback(self, sdRef, flags, errorCode, name, regtype, domain):
    if errorCode == pybonjour.kDNSServiceErr_NoError:
      print '...bonjour listener', name+'.'+regtype+domain, 'now listening on', self.rpc_port

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
          torrent = lt.bdecode(f.read())
          torrent_info = torrent.get('info')  if torrent else None
          st_size = torrent_info.get('length') if torrent_info else 0
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
      self.should_push = True
      return ret

  def read(self, path, size, offset, fh):
    with self.rwlock:
      print 'path in self.bt_in_progress', path in self.bt_in_progress
      if path in self.bt_in_progress:
        print '.. in progress'
        h = self.bt_handles[path]
        torrent_info = h.get_torrent_info()
        piece_length = torrent_info.piece_length()
        num_pieces = torrent_info.num_pieces()
        start_index = offset // piece_length
        end_index = (offset+size) // piece_length
        print 'pieces', start_index, end_index
        #for i in range(start_index, end_index+1):
        #  h.piece_priority(i, 8)
        #print 'piece_priorities set'
        for i in range(start_index, min(end_index+1,num_pieces)):
          print 'waiting for', i
          while not h.have_piece(i):
            time.sleep(1)
          print 'we have', i
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
          dat_fn = os.path.join(self.dat, name[:2], name)
          if not os.path.isfile(dat_fn):
            self.__add_torrent_and_wait(path, t)
          return os.open(dat_fn, flags)
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
      self.should_push = True
      self.__add_torrent(tdata, path)
    except Exception as e:
      traceback.print_exc()
      raise e
      
  def __add_torrent(self, torrent, path):
    uid = torrent['info']['name']
    info = lt.torrent_info(torrent)
    dat_file = os.path.join(self.dat, uid[:2], uid)
    #print 'dat_file', dat_file
    if not os.path.isdir(os.path.dirname(dat_file)): os.mkdir(os.path.dirname(dat_file))
    h = self.bt_session.add_torrent({'ti':info, 'save_path':os.path.join(self.dat, uid[:2])})
    #h = self.bt_session.add_torrent(info, os.path.join(self.dat, uid[:2]), storage_mode=lt.storage_mode_t.storage_mode_sparse)
    print 'added', uid
    self.bt_handles[path] = h

    
  def rename(self, old, new):
    with self.rwlock:
      self.repo.hg_rename(self.hgdb+old, self.hgdb+new)
      self.repo.hg_commit('rename %s to %s' % (old,new), files=[self.hgdb+old, self.hgdb+new])
      self.should_push = True

#  rmdir = os.rmdir

  def rmdir(self, path):
    with self.rwlock:
      self.repo.hg_remove(self.hgdb+path+'/.__dpdfs_dir__')
      self.repo.hg_commit('rmdir %s' % path, files=[self.hgdb+path+'/.__dpdfs_dir__'])
      self.should_push = True
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
      self.should_push = True

  def write(self, path, data, offset, fh):
    with self.rwlock:
      os.lseek(fh, offset, 0)
      return os.write(fh, data)


def get_torrent_dict(fn):
  if not os.path.isfile(fn): return
  with open(fn, 'rb') as f:
    return lt.bdecode(f.read())




resolved = []





if __name__ == '__main__':
  create = '--create' in sys.argv
  args = [x for x in sys.argv if not x.startswith('-')]
  if len(args) != 4:
    print('usage: %s [--create] <name> <root> <mountpoint>' % sys.argv[0])
    sys.exit(1)

  fuse = FUSE(DPDFS(args[1], args[2], create=create), args[3], foreground=True)

