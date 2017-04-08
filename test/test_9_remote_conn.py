from unittest.mock import MagicMock
import shutil
from os.path import join
from functools import partial
from threading import Thread
from queue import Queue


from sarch.common import CONFIG
from .common import TestBase
from sarch.remote_ssh import RemoteConnection, RemoteSSHServer, RemoteSSH, SA_SYNC_Exception_SSH_Server_Error
from sarch.database import Meta


class ThreadedRunner():
   
   def __init__(self, worker_fun):
      self.worker = Thread( target=worker_fun, daemon=True )   
      self.worker.start()
   
   
   def wait(self):
      self.worker.join( timeout=1.0 )
      
   
   
      


class FakePipe:
   
   def __init__( self ) -> None:
      self.queue = Queue() # type: Queue
      
   def read1( self, size ) -> bytearray:
      return self.queue.get(  timeout=1 ) 
   
   def flush(self):
      pass
   
   def write( self, data ) -> None:
      self.queue.put( bytearray( data ) )
      
   
def chunks(l):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), 2):
        yield l[i:i + 2]


      
      
class TestRemoteConn( TestBase ):
   """ Test remote ssh connection with spoofed setup; use the current repository as the server and
   the client as fake """
   
   def remote_open( self ) -> None:
      
      self.pipe_server_in = FakePipe()
      self.pipe_client_in = FakePipe()
      
      self.client = RemoteConnection( self.pipe_client_in, self.pipe_server_in,  ) # type: ignore 
      self.server_raw = RemoteSSHServer( self.repo.db, self.repo.fs,  self.pipe_server_in, self.pipe_client_in, ) # type: ignore
      
      # Function to be called when client runs out of data
      self.remote = RemoteSSH("other")
      self.remote.conn = self.client
      self.remote.ssh  = MagicMock() # type: ignore
      self.remote.ssh.communicate =  MagicMock( return_value = (bytes("STDOUT", "utf8"),bytes("STDERR", "utf8") ) ) # type: ignore
      
      # Start server thread
      self.server = ThreadedRunner( self.server_raw.run )
      self.remote._database_open()
      
   def close( self ):
      self.remote.close()
      self.server.wait()
      
   def test_open( self ):
      self.remote_open()
      self.close()
      
      
   def test_get_save(self) -> None:
      self.remote_open()
      db = self.remote.database_get()
      
      (n_commits, n_stor, n_staging) = db.get_table_sizes()
      meta = db.meta_get("FOO")
      meta.checksum = "#INVALID"
      db.meta_set( meta )
      
      self.remote.database_save()
      self.close()
      
      # we know we are actually using same database
      meta  = self.repo.db_get("FOO")
      self.assertEqual( meta.checksum, "#INVALID" )
      
   def test_get_dirty( self ) -> None:
      self.repo.main("rm", "FOO")
      self.repo.main("status")
      self.repo.open_db()
      with self.assertRaises(SA_SYNC_Exception_SSH_Server_Error):
         self.remote_open()
      self.close()
      
   def open_and_get_foo(self):
     self.remote_open()
     meta = self.remote.db.meta_get("FOO")
     return meta
  
   def test_file_get_set(self) -> None:
     meta = self.open_and_get_foo()
     fid = self.remote.file_get(  meta )
     data=bytearray()
     for data_loop in fid:
        data += data_loop
     self.repo.file_check("FOO", exists = True, checksum=meta.checksum )
     meta.filename = "FOO_SET"
     self.remote.file_set( meta, chunks(data) )
     self.close()
     self.repo.file_check("FOO_SET", exists = True, checksum=meta.checksum )
   
   def test_set_untracked( self ) -> None:
      self.remote_open()
      self.repo.file_make("NEW_FILE")
      meta = Meta("NEW_FILE")
      self.repo.fs.meta_update( meta )
      self.repo.file_make("NEW_FILE", content="INVALIDFOO"*16 )
      with self.assertRaises(SA_SYNC_Exception_SSH_Server_Error):
         self.remote.file_set( meta, chunks( bytes("FOO"*16,"utf8") ))
      self.close()
      
         
   def test_file_move_untracked(self) -> None:
      meta = self.open_and_get_foo()
      self.repo.file_make("FOO_COPY")
      meta_copy  = meta.copy()
      meta_copy.filename = "FOO_COPY"
      with self.assertRaises(SA_SYNC_Exception_SSH_Server_Error):
         self.remote.file_move( meta, meta_copy )
      self.close()

   def test_file_move(self) -> None:
     meta = self.open_and_get_foo()
     meta_new = meta.copy()
     meta_new.filename = "FOO_MOVED"
     self.remote.file_move( meta , meta_new )
     self.close()
     self.repo.file_check("FOO", exists = False )
     self.repo.file_check("FOO_MOVED", exists = True, checksum=meta.checksum)
     

   def test_file_del_copy(self) -> None:
     meta_f = self.open_and_get_foo()
     meta_b = self.remote.db.meta_get("BAR")
     meta_f2 = meta_f.copy()
     meta_f2.filename = "FOO_COPY"
     self.remote.file_copy(  meta_f, meta_f2 )
     self.remote.file_del( meta_b )
     self.close()
     
     self.repo.file_check("FOO", exists = True, checksum=meta_f.checksum )
     self.repo.file_check("FOO_COPY", exists = True, checksum=meta_f.checksum )
     self.repo.file_check("BAR", exists = False)


   def test_cancel_resumed(self) -> None:
     meta = Meta("NEW_FILE")
     content = "THIS WILL BE IN DISK"
     self.repo.file_make("NEW_FILE", content=content)
     self.repo.fs.meta_update( meta )
     self.remote_open()
     self.remote.file_set( meta, chunks(content) )
     self.close()
     
     
