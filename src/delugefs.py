#!/usr/bin/env python

import os, errno, sys, threading, collections, uuid, shutil, traceback, random, select, threading, time, socket, multiprocessing, stat, datetime, statvfs
from fuse import FUSE, FuseOSError, Operations, LoggingMixIn
import libtorrent as lt
import hgapi as hg
import pybonjour
import jsonrpc


SECONDS_TO_NEXT_CHECK = 120

class Peer(object):
  def __init__(self, service_name, host, port):
    self.service_name = service_name
    self.host = host
    self.addr = socket.gethostbyname(host)
    self.port = port
    self.server = jsonrpc.ServerProxy(jsonrpc.JsonRpc20(), jsonrpc.TransportTcpIp(addr=(host,port)))
    self.hg_port = self.server.get_hg_port()
    self.bt_port = self.server.get_bt_port()

class DelugeFS(LoggingMixIn, Operations):
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
    self.next_time_to_check_for_undermirrored_files = datetime.datetime.now() + datetime.timedelta(0,10+random.randint(0,30))
    
    if not os.path.isdir(self.root):
      os.mkdir(self.root)
    
    bt_start_port = random.randint(10000, 20000)
    self.bt_session = lt.session()
    self.bt_session.listen_on(bt_start_port, bt_start_port+10)
    self.bt_port = self.bt_session.listen_port()
    self.bt_session.start_lsd()
    self.bt_session.start_dht()
    print 'libtorrent listening on:', self.bt_port
    self.bt_session.add_dht_router('localhost', 10670)
    print '...is_dht_running()', self.bt_session.dht_state()
    
    t = threading.Thread(target=self.__start_listening_bonjour)
    t.daemon = True
    t.start()
    print 'give me a sec to look for other peers...'
    time.sleep(2)
    
    self.repo = hg.Repo(self.hgdb)
    cnfn = os.path.join(self.hgdb, '.__delugefs__', 'cluster_name')
    if create:
      if os.listdir(self.root):
        raise Exception('--create specified, but %s is not empty' % self.root)
      if self.peers:
        raise Exception('--create specified, but i found %i peer%s using --id "%s" already' % (len(self.peers), 's' if len(self.peers)>1 else '', self.name))
      os.mkdir(self.hgdb)
      self.repo.hg_init()
      os.mkdir(os.path.join(self.hgdb, '.__delugefs__'))
      with open(cnfn, 'w') as f:
        f.write(self.name)
      self.repo.hg_add(cnfn)
      self.repo.hg_commit(cnfn)
    else:
      if os.path.isfile(cnfn):
        with open(cnfn, 'r') as f:
          existing_cluster_name = f.read().strip()
          if existing_cluster_name != self.name:
            raise Exception('a cluster root exists at %s, but its name is "%s", not "%s"' % (self.root, existing_cluster_name, self.name))
      else:
        if os.listdir(self.root):
          raise Exception('root %s is not empty, but no cluster was found' % self.root)
        if not self.peers:
          raise Exception('--create not specified, no repo exists at %s and no peers of cluster "%s" found' % (self.root, self.name))
        try:
          apeer = self.peers[iter(self.peers).next()]
          #server = jsonrpc.ServerProxy(jsonrpc.JsonRpc20(), jsonrpc.TransportTcpIp(addr=addr))
          #remote_hg_port = apeer.server.get_hg_port()
          if not os.path.isdir(self.hgdb): os.mkdir(self.hgdb)
          self.repo.hg_clone('http://%s:%i' % (apeer.host, apeer.hg_port))
        except Exception as e:
          if os.path.isdir(os.path.join(self.hgdb, '.hg')):
            shutil.rmtree(self.hgdb)
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


  def __write_active_torrents(self):
    try:
      with open(os.path.join(self.hgdb, '.__delugefs__', 'active_torrents'), 'w') as f:
        for path, h in self.bt_handles.items():
          s = h.status()
#          torrent_peers = h.get_peer_info()
#          print 'torrent_peers', torrent_peers
#          if len(torrent_peers) < 1:
#            print 'only', len(torrent_peers), 'peer for', path
#            if self.peers:
#              peer = self.peers.values()[random.randint(0,len(self.peers)-1)]
#              peer.server.please_mirror(path)
          #if s.state==5 and s.download_rate==0 and s.upload_rate==0: continue
          state_str = ['queued', 'checking', 'downloading metadata', 'downloading', 'finished', 'seeding', 'allocating']
          f.write('%s is %.2f%% complete (down: %.1f kb/s up: %.1f kB/s peers: %d) %s\n' % \
              (path, s.progress * 100, s.download_rate / 1000, s.upload_rate / 1000, \
              s.num_peers, ""))
    except Exception as e:
      traceback.print_exc()
  
  def __check_for_undermirrored_files(self):
    if self.next_time_to_check_for_undermirrored_files > datetime.datetime.now(): return
    try:
      print 'check_for_undermirrored_files @', datetime.datetime.now()
      my_uids = set(self.get_active_info_hashes())
      counter = collections.Counter(my_uids)
      peer_free_space = {'__self__': self.get_free_space()}
      uid_peers = collections.defaultdict(set)
      for uid in my_uids:
        uid_peers[uid].add('__self__')
      for peer_id, peer in self.peers.items():
        for s in peer.server.get_active_info_hashes():
          counter[s] += 1
          uid_peers[s].add(peer_id)
        peer_free_space[peer_id] = peer.server.get_free_space()
      print 'counter', counter
      print 'peer_free_space', peer_free_space
      if len(peer_free_space) < 2:
        print "can't do anything, since i'm the only peer!"
        return

      for root, dirs, files in os.walk(self.hgdb):
        #print 'root, dirs, files', root, dirs, files
        if root.startswith(os.path.join(self.hgdb, '.hg')): continue
        if root.startswith(os.path.join(self.hgdb, '.__delugefs__')): continue
        for fn in files:
          if fn=='.__delugefs_dir__': continue
          fn = os.path.join(root, fn)
          e = get_torrent_dict(fn)
          uid = e['info']['name']
          size = e['info']['length']
          path = fn[len(self.hgdb):]
          if counter[uid] < 2:
            peer_free_space_list = sorted(peer_free_space.items(), lambda x,y: x[1]<y[1])
            best_peer_id = peer_free_space_list[0][0]
            if uid in my_uids and best_peer_id=='__self__':
              best_peer_id = peer_free_space_list[1][0]
            peer_free_space[best_peer_id] -= size
            print 'need to rep', path, 'to', best_peer_id
            if '__self__'==best_peer_id:
              self.please_mirror(path)
            else:
              self.peers[best_peer_id].server.please_mirror(path)
            print 'peer_free_space_list', peer_free_space_list
          if counter[uid] > 2:
            print 'uid_peers', uid_peers
            peer_free_space_list = sorted([x for x in peer_free_space.items() if x[0] in uid_peers[uid]], lambda x,y: x[1]>y[1])
            print 'peer_free_space_list2', peer_free_space_list
            best_peer_id = peer_free_space_list[0][0]
            if '__self__'==best_peer_id:
              self.please_stop_mirroring(path)
            else:
              self.peers[best_peer_id].server.please_stop_mirroring(path)
            print 'please_stop_mirroring', path, 'sent to', best_peer_id


    except Exception as e:
      traceback.print_exc()
    self.next_time_to_check_for_undermirrored_files = datetime.datetime.now() + datetime.timedelta(0,SECONDS_TO_NEXT_CHECK+random.randint(0,30))
    

  def __monitor(self):
    time.sleep(3)
    while True:
      #print '='*80
      self.__write_active_torrents()
      self.__check_for_undermirrored_files()
      time.sleep(random.randint(3,7))
        
        

  
  def __load_local_torrents(self):
    #print 'self.hgdb', self.hgdb
    for root, dirs, files in os.walk(self.hgdb):
      #print 'root, dirs, files', root, dirs, files
      if root.startswith(os.path.join(self.hgdb, '.hg')): continue
      if root.startswith(os.path.join(self.hgdb, '.__delugefs__')): continue
      for fn in files:
        if fn=='.__delugefs_dir__': continue
        fn = os.path.join(root, fn)
        print 'loading torrent', fn
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
    browse_sdRef = pybonjour.DNSServiceBrowse(regtype="_delugefs._tcp", callBack=self.__bonjour_browse_callback)
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
      if not (fullname.startswith(self.name+'__') and '._delugefs._tcp.' in fullname):
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
      
  def please_mirror(self, path):
    print 'please_mirror', path
    fn = self.hgdb+path
    torrent = get_torrent_dict(fn)
    self.__add_torrent(torrent, path)
    
  def please_stop_mirroring(self, path):
    print 'got please_stop_mirroring', path
    
   
  def get_active_info_hashes(self):
    self.next_time_to_check_for_undermirrored_files = datetime.datetime.now() + datetime.timedelta(0,SECONDS_TO_NEXT_CHECK+random.randint(0,30))
    try:
      active_info_hashes = []
      for h in self.bt_handles.values():
        active_info_hashes.append(str(h.get_torrent_info().name()))
      print 'active_info_hashes', active_info_hashes
      return active_info_hashes
    except:
      traceback.print_exc()
      
  def get_free_space(self):
    f = os.statvfs(self.root)
    return f[statvfs.F_BSIZE] * f[statvfs.F_BFREE]
  
  def __register(self):
    #return

    server = jsonrpc.Server(jsonrpc.JsonRpc20(), jsonrpc.TransportTcpIp(addr=('', self.rpc_port))) #, logfunc=jsonrpc.log_file("myrpc.%i.log"%self.rpc_port)
    server.register_function(self.get_hg_port)
    server.register_function(self.get_bt_port)
    server.register_function(self.you_should_pull_from)
    server.register_function(self.please_mirror)
    server.register_function(self.get_active_info_hashes)
    server.register_function(self.get_free_space)
    server.register_function(self.please_stop_mirroring)
    
    
    t = threading.Thread(target=server.serve)
    t.daemon = True
    t.start()

    t = threading.Thread(target=self.repo.hg_serve, args=(self.hg_port,))
    t.daemon = True
    t.start()
    print 'http://localhost:%i/' % self.hg_port

    
    print 'registering bonjour listener...'
    self.bj_name = self.name+'__'+uuid.uuid4().hex
    bjservice = pybonjour.DNSServiceRegister(name=self.bj_name, regtype="_delugefs._tcp", 
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
    if path.endswith('/.__delugefs_dir__'): raise FuseOSError(errno.EACCES)
    return super(DelugeFS, self).__call__(op, path, *args)

  def access(self, path, mode):
    if not os.access(self.hgdb+path, mode):
      raise FuseOSError(errno.EACCES)

#  chmod = os.chmod
#  chown = os.chown

  def create(self, path, mode):
    with self.rwlock:
      if path.startswith('/.__delugefs__'): return 0
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
        if not path.startswith('/.__delugefs__'):
          with open(fn, 'rb') as f:
            torrent = lt.bdecode(f.read())
            torrent_info = torrent.get('info')  if torrent else None
            st_size = torrent_info.get('length') if torrent_info else 0
    st = os.lstat(fn)
    ret = dict((key, getattr(st, key)) for key in ('st_atime', 'st_ctime',
            'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size', 'st_uid'))
    if path.startswith('/.__delugefs__'):
      ret['st_mode'] = ret['st_mode'] & ~(stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH)
    if st_size is not None:
      ret['st_size'] = st_size
    return ret

  getxattr = None
  
  def link(self, target, source):
    with self.rwlock:
      if path.startswith('/.__delugefs__'): return 0
      return os.link(source, target)

  listxattr = None
#  mknod = os.mknod

  def mkdir(self, path, flags):
    with self.rwlock:
      if path.startswith('/.__delugefs__'): return 0
      fn = self.hgdb+path
      ret = os.mkdir(fn, flags)
      with open(fn+'/.__delugefs_dir__','w') as f:
        f.write("hg doesn't track empty dirs, so we add this file...")
      self.repo.hg_add(fn+'/.__delugefs_dir__')
      self.repo.hg_commit('mkdir %s' % path, files=[fn+'/.__delugefs_dir__'])
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
        priorities = h.piece_priorities()
        print 'priorities', priorities
        for i in range(start_index, min(end_index+1,num_pieces)):
          priorities[i] = 7
        h.prioritize_pieces(priorities)
        print 'priorities', priorities
        #  h.piece_priority(i, 8)
        #print 'piece_priorities set'
        for i in range(start_index, min(end_index+1,num_pieces)):
          print 'waiting for', i
          while not h.have_piece(i):
            time.sleep(1)
          print 'we have', i
      os.lseek(fh, offset, 0)
      ret = os.read(fh, size)
      #print 'ret', ret
      return ret

  def open(self, path, flags):
    with self.rwlock:
      fn = self.hgdb+path
      if not (flags & (os.O_WRONLY | os.O_RDWR | os.O_APPEND | os.O_CREAT | os.O_EXCL | os.O_TRUNC)):
        if path.startswith('/.__delugefs__'):
          return os.open(fn, flags)
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
      if path.startswith('/.__delugefs__'): return 0
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
      return ['.', '..'] + [x for x in os.listdir(self.hgdb+path) if x!=".__delugefs_dir__" and x!='.hg']

#    readlink = os.readlink

  def release(self, path, fh):
    with self.rwlock:
      ret = os.close(fh)
      print 'ret', ret, path
      if path in self.open_files:
        self.finalize(path, self.open_files[path])
        del self.open_files[path]
      if path in self.bt_in_progress:
        h = self.bt_handles[path]
        priorities = h.piece_priorities()
        #h.prioritize_pieces([0 for x in priorities])
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
      t.set_creator("DelugeFS");
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
      if old.startswith('/.__delugefs__'): return 0
      if new.startswith('/.__delugefs__'): return 0
      self.repo.hg_rename(self.hgdb+old, self.hgdb+new)
      self.repo.hg_commit('rename %s to %s' % (old,new), files=[self.hgdb+old, self.hgdb+new])
      self.should_push = True

#  rmdir = os.rmdir

  def rmdir(self, path):
    with self.rwlock:
      if path.startswith('/.__delugefs__'): return 0
      self.repo.hg_remove(self.hgdb+path+'/.__delugefs_dir__')
      self.repo.hg_commit('rmdir %s' % path, files=[self.hgdb+path+'/.__delugefs_dir__'])
      self.should_push = True
      #return os.rmdir(self.hgdb+path)

  def statfs(self, path):
        stv = os.statvfs(self.hgdb+path)
        return dict((key, getattr(stv, key)) for key in ('f_bavail', 'f_bfree',
            'f_blocks', 'f_bsize', 'f_favail', 'f_ffree', 'f_files', 'f_flag',
            'f_frsize', 'f_namemax'))

  def symlink(self, target, source):
    with self.rwlock:
      if target.startswith('/.__delugefs__'): return 0
      if src.startswith('/.__delugefs__'): return 0
      ret = os.symlink(source, target)
      return ret
        

  def truncate(self, path, length, fh=None):
    with self.rwlock:
      if path.startswith('/.__delugefs__'): return 0
      with open(os.path.join(self.tmp, self.open_files[path]), 'r+') as f:
        f.truncate(length)

#  utimens = os.utime

  def unlink(self, path):
    with self.rwlock:
      if path.startswith('/.__delugefs__'): return 0
      with open(self.hgdb+path, 'rb') as f:
        torrent = lt.bdecode(f.read())
        torrent_info = torrent.get('info')  if torrent else None
        name = torrent_info.get('name') if torrent_info else 0
        dfn = os.path.join(self.dat, name[:2], name)
        if os.path.isfile(dfn):
          os.remove(dfn)
          print 'deleted', dfn
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



def usage(msg):
  print 'ERROR:', msg
  print('usage: %s [--create] --cluster <clustername> --root <root> [--mount <mountpoint>]' % sys.argv[0])
  print '  cluster: any string uniquely identifying the desired cluster'
  print '  root: path to backend storage to use'
  print '  mount: path to FUSE mount location'
  sys.exit(1)


if __name__ == '__main__':

  config = {}
  k = None
  for s in sys.argv:
    if s.startswith('--'):
      if k:  config[k] = True
      k = s[2:]
    else:
      if k:
        config[k] = s
        k = None
        
  if not 'cluster' in config:
    usage('cluster name not set')
  if not 'root' in config:
    usage('root not set')
  

  server = DelugeFS(config['cluster'], config['root'], create=config.get('create'))
  if 'mount' in config:
    fuse = FUSE(server, config['mount'], foreground=True)
  else:
    while True:
      time.sleep(60)

