
import json
import sys

from typing import Iterable, Union, Dict, cast, IO, Any, Tuple
from subprocess import Popen,PIPE

from .database import DatabaseBase, Meta, SA_DB_Exception_NotFound
from .database_json import DatabaseJson
from .filesystem import Filesystem

from .common import CONFIG, print_debug, print_info, print_error
from .remote import Remote, check_database, check_file_equal , SA_SYNC_Exception, SA_SYNC_Exception_Cancelled, Filestatus


class SA_SYNC_Exception_SSH( SA_SYNC_Exception ):
   pass
class SA_SYNC_Exception_SSH_Server_Error( SA_SYNC_Exception_SSH ):
   pass

class SA_SYNC_Exception_SSH_Connection_Closed(SA_SYNC_Exception):
   pass


ConnValue = Union[str,int]
MetaPacked = Tuple[ str, str, int ]

class RemoteConnectionCMDS:
   CMD_HANDSHAKE  = "hello"
   CMD_CLOSE = "close"
   CMD_GET = "get"
   CMD_SET = "set"
   CMD_DEL = "del"
   CMD_MOVE = "mov"
   CMD_COPY = "cpy"
   CMD_DB_GET = "dbg"
   CMD_DB_SET = "dbs"

class RemoteConnection(RemoteConnectionCMDS):
   
   RSP_STATUS_KEY = "status"
   RSP_STATUS_OK  = "ok"
   RSP_STATUS_DONE = "done"
   
   RSP_DATABASE_KEY = "db"
   DATA_LEN_KEY = "len"
   
   PROTO_ENDMARKER  =  bytes.fromhex("00")
   PROTO_KEY_CMD    = "cmd"
   PROTO_KEY_PARAMS = "par"
   
   KNOWN_COMMANDS  = [ x for x in dir( RemoteConnectionCMDS ) if x.startswith("CMD_") ]
   COMMANDS_LOOKUP = { getattr(RemoteConnectionCMDS, x) : x  for x in KNOWN_COMMANDS }
     
   def __init__(self, pipe_in : IO[bytes] , pipe_out : IO[bytes]  ) -> None:
      self.pipe_in = pipe_in
      self.pipe_out = pipe_out
      self.clear_buffer()
      
   def clear_buffer( self ):
      self.data =  bytearray()
   
   @staticmethod
   def meta_package( meta : Meta ) ->  MetaPacked:
      return ( meta.filename, meta.checksum, meta.modtime )
   
   @staticmethod
   def meta_unpack( values : MetaPacked ) -> Meta:
      meta = Meta( values[0] )
      meta.checksum = values[1]
      meta.modtime = values[2]
      return meta
      
   def _construct_object( self, obj : Dict[str, Any ] ) -> bytes:
      data_str = json.dumps( obj) + "\0" 
      return bytes( data_str, "utf8" )

   def resp_wait_object( self ) -> Dict[str, Any ]:
      
      index = self.data.find( self.PROTO_ENDMARKER )
      if index <= 0:
         while True:
           self._read_input(CONFIG.DATA_BLOCK_SIZE)
           index = self.data.find( self.PROTO_ENDMARKER )
           if index >= 0:
              break
      obj_raw  = (self.data[0:index]).decode("utf8")
      self.data = self.data[(index+1):]

      
      obj = json.loads( obj_raw )
      return obj
   
   def _send( self, data : bytes ):
      self.pipe_out.write(data)
      self.pipe_out.flush()
      
   def _read_input(self, nbytes):
      data_in = self.pipe_in.read1(nbytes)
      self.data += data_in
      if len(data_in) == 0:
        raise SA_SYNC_Exception_SSH_Connection_Closed("Connection closed")

   def resp_wait_count( self, count : int ) -> bytes:
      while len( self.data ) < count:
         self._read_input(  count  - len(self.data) )
      to_ret    = self.data[0:count]
      self.data = self.data[count:]
      return to_ret
   
   def wait_for_ack( self ) -> Dict[str, ConnValue]:
      resp_json = self.resp_wait_object()
      resp_status = resp_json[ self.RSP_STATUS_KEY ]
      if resp_status == self.RSP_STATUS_OK or resp_status == self.RSP_STATUS_DONE:
         return resp_json
      raise SA_SYNC_Exception_SSH_Server_Error("Error on remote '%s' " % ( resp_status ) )
      
   def send( self, *parameters : Union[ ConnValue, MetaPacked ] ) -> Dict[str, ConnValue]:
      assert( len(self.data) == 0 )
      json_bytes = self._construct_object( { self.PROTO_KEY_CMD : parameters[0], self.PROTO_KEY_PARAMS : parameters[1:] }  ) 
      self._send( json_bytes )
      return self.wait_for_ack()
   
   def send_obj( self, obj : Dict[str, Any] ) -> None:
       header_bytes = self._construct_object( obj )
       self._send( header_bytes ) 
   
   def data_send( self, data_source : Iterable [bytes] ) -> None:
      for package in data_source:
         data_len = len( package )
         self.send_obj( { self.DATA_LEN_KEY : data_len } )
         self._send( package )
      # And then say that we are done
      self.send_obj( { self.DATA_LEN_KEY : 0 } )
              
      
   def data_receive( self ) -> Iterable [bytes]:
      while True:
         header = self.resp_wait_object()
         data_len = int( header[ self.DATA_LEN_KEY ] ) 
         
         if data_len == 0: # We are done!
            return 
         
         # Ok, we received header that says that there is data_len amount of raw data coming in.
         # Yield it in buffer sized blocks
         data_count = 0
         while data_count < data_len:
            to_get = min( data_len - data_count, CONFIG.DATA_BLOCK_SIZE )
            data_package = self.resp_wait_count( to_get )
            data_count += to_get
            yield data_package 
         

class RemoteSSHServerConnClose( SA_SYNC_Exception ):
   pass

import sys

class RemoteSSHServer:
   
   def __init__( self, database : DatabaseBase, filesystem : Filesystem, pipe_in : IO[bytes], pipe_out : IO[bytes] ) -> None:
      self.db   = database
      self.fs   = filesystem
      self.conn = RemoteConnection( pipe_in, pipe_out )
      self.commands = { cmd.lower() : getattr(self, "serve_" + cmd.lower() ) for cmd in RemoteConnection.KNOWN_COMMANDS }
      self.last_sent_error = None # type: str

   def send_response( self, values : Dict[ str, Any ] = None, error : str = None ):
      to_send = { }
      if error == None:
         status_value = RemoteConnection.RSP_STATUS_OK
         self.last_sent_error = None
      else:
         status_value = error
         self.last_sent_error = error
         
      to_send[ RemoteConnection.RSP_STATUS_KEY ] = status_value
      if values != None:
         to_send.update( values )
      self.conn.send_obj( to_send )


   def serve_cmd_handshake( self, version : str ) -> None:
      try:
         check_database( self.db )
      except SA_SYNC_Exception_Cancelled as err:
         self.send_response( error=str(err) )
         return
      self.fs.trash_clear()
      self.send_response( { "version" : CONFIG.VERSION } )
      
   def serve_cmd_close( self ) -> None:
      self.send_response()
      self.fs.trash_clear()
      raise RemoteSSHServerConnClose()
   
   def serve_cmd_get( self, source : MetaPacked ) -> None:
      meta_source = RemoteConnection.meta_unpack( source )
      fid = self.fs.file_read( meta_source.filename )
      self.send_response()
      self.conn.data_send( fid )
   
   def _check_file_done( self, meta : Meta ) -> bool:
      status = check_file_equal( meta, self.db, self.fs ) 
      
      if status == Filestatus.FILE_OVERWRITE_OK:
         return False
      elif status == Filestatus.FILE_EQUAL:
         self.send_response( error = RemoteConnection.RSP_STATUS_DONE )
         return True
      else:
         self.send_response( error=str(status) )
         return True
      
   def serve_cmd_set( self, target : MetaPacked ) -> None :
      meta_target = RemoteConnection.meta_unpack( target )
      if self._check_file_done( meta_target ):
         return
      self.send_response() # We must ack the command before data starts flowing
      data_source = self.conn.data_receive()
      self.fs.file_create( meta_target, data_source )
      
   def serve_cmd_del( self, target : MetaPacked )  -> None:
      meta_target = RemoteConnection.meta_unpack( target )
      self.fs.file_del( meta_target.filename,  missing_ok = True )
      self.send_response()
      
            
   def _get_check_source_target( self, source : MetaPacked, target : MetaPacked ) -> Tuple[Meta, Meta]:
      meta_source = RemoteConnection.meta_unpack( source )
      meta_target = RemoteConnection.meta_unpack( target )
      
      if self._check_file_done( meta_target ) :
         return (None, meta_target)
      
      return (meta_source, meta_target)
   
   def serve_cmd_move( self, source : MetaPacked, target : MetaPacked )  -> None:
      meta_source, meta_target = self._get_check_source_target( source, target )
      if meta_source == None:
         self.fs.file_del( meta_target.filename,  missing_ok = True )
         return 
      self.fs.move( meta_source.filename, meta_target.filename, create_dirs=True, modtime=meta_target.modtime )
      self.send_response()
   
   def serve_cmd_copy( self, source : MetaPacked , target : MetaPacked )  -> None:
      meta_source, meta_target = self._get_check_source_target( source, target )
      if meta_source == None:
         return 
      fid = self.fs.file_read( meta_source.filename )
      self.fs.file_create( meta_target, fid )
      self.send_response()
      
   def serve_cmd_db_get( self ) -> None:
      db_as_json = self.db.json_dumps()
      self.send_response( { RemoteConnection.RSP_DATABASE_KEY : db_as_json } )
   
   def serve_cmd_db_set( self, db_json_str ) -> None: 
      self.db.json_loads( db_json_str )
      self.db.save()
      self.send_response()
      
   def run( self ) -> bool:
      while True:
         try:
            cmd_obj = self.conn.resp_wait_object()
            
         except SA_SYNC_Exception_SSH_Connection_Closed as err:
            if self.last_sent_error != None:
               break # This is complitely ok. The other end closed connection in response to our error
            raise err from err
            
         command_str = str( cmd_obj[ self.conn.PROTO_KEY_CMD ] )
         
         command = RemoteConnection.COMMANDS_LOOKUP[ command_str ].lower()
         params  = cmd_obj[ self.conn.PROTO_KEY_PARAMS ] # type: List[Any]
         
         fun_to_call = self.commands[ command ] 
         try:
            fun_to_call( *params )
         except RemoteSSHServerConnClose:
            break
         
  
      
      
def remote_ssh_server( database : DatabaseBase, filesystem : Filesystem, pipe_in : IO[bytes], pipe_out : IO[bytes] ):
   """ This is the remote ssh server that responds to all commands, while connection is open """
   server = RemoteSSHServer( database, filesystem, pipe_in, pipe_out )
   server.run()

   return 0
      
class RemoteSSHServerFileNotEqual( SA_SYNC_Exception_SSH ):
   pass

class RemoteSSH( Remote ):
   
   def file_get( self, source : Meta ) -> Iterable [bytes]:
      self.conn.send( self.conn.CMD_GET, self.conn.meta_package( source ) ) 
      yield from self.conn.data_receive()
      
   def file_set( self, target : Meta, content: Iterable [bytes] ) -> None:
      ret = self.conn.send( self.conn.CMD_SET, self.conn.meta_package( target )  )
      
      if ret[ RemoteConnection.RSP_STATUS_KEY ] == RemoteConnection.RSP_STATUS_DONE:
         return
      self.conn.data_send( content )
   
   def file_del( self, target : Meta ) -> None:
      self.conn.send( self.conn.CMD_DEL, self.conn.meta_package( target ) )
      
   def file_move( self, source : Meta, target : Meta ) -> None:
      self.conn.send( self.conn.CMD_MOVE, self.conn.meta_package( source ), self.conn.meta_package( target ) )

   def file_copy( self, source : Meta, target : Meta ) -> None:
      self.conn.send( self.conn.CMD_COPY, self.conn.meta_package( source ), self.conn.meta_package( target ) )
      
   def database_get( self ) -> DatabaseBase:
      return self.db
   
   def _database_open( self ) -> None:
      self.db = DatabaseJson()
      self.conn.send( self.conn.CMD_HANDSHAKE, CONFIG.VERSION )
      print_debug( "Connection ok. Fetching database .. "  )
      resp = self.conn.send( self.conn.CMD_DB_GET )
      self.db.json_loads( str( resp[ self.conn.RSP_DATABASE_KEY ] ) )
      
   def database_save( self ) -> None :
      self.conn.send( self.conn.CMD_DB_SET, self.db.json_dumps() )
   
   def _close_raw( self ) -> None:
      
      def print_remote_error( prefix, lines ):
         for line in lines.split("\n"):
            line = line.strip()
            if len(line) == 0 :
               continue
            print_error( prefix + line )
            
      (stdout, stderr ) = self.ssh.communicate( input=None, timeout=5)
      if len(stdout) > 0:
         print_remote_error("Remote stdout:", stdout.decode("utf-8") ) 
      if len(stderr) > 0:
         print_remote_error("Remote stderr:", stderr.decode("utf-8") ) 
      
   def close( self, assume_ok = True ) -> None:
      self.conn.send( self.conn.CMD_CLOSE )
      self._close_raw()
      
         
      
      
      

   def open( self, url : str ):
      """ Open ssh connection to remote, and execute there sarch sync command, 
         :param url: is assumed to be like  ssh://username@host.foo.com:/my/path/to/target" """
          
      assert( url.startswith("ssh://") )
      # replace the ssh
      url_parts = url[6:].split(":", 1)
      if len( url_parts ) != 2:
         raise SA_SYNC_Exception_SSH("Invalid url '%s'. It must be in format 'ssh://<hostname>:<path to repository>'" % url )
      
      username_n_host = url_parts[0]
      target_path     = url_parts[1]
      print_debug( "Opening connection to %s .. " % username_n_host )
      self.ssh = Popen( ( CONFIG.SSH_COMMAND, username_n_host, "sarch", "_server_mode", target_path ), 
                         stdin=PIPE, stdout=PIPE,stderr=PIPE )
      self.conn = RemoteConnection( self.ssh.stdout, self.ssh.stdin, )
      print_info("Connection opened, fetching database .. ")
      #self.ssh.stdin.raw.write( bytes('{"cmd": "hello", "par": ["1.0.0"]}\0{"cmd": "close", "par": []}\0',"utf8") )
      try:
         self._database_open()
      except SA_SYNC_Exception_SSH as err:
         print_error("Database open failed: %s" % err )
         self._close_raw()
         raise SA_SYNC_Exception_SSH("Could not fetch database")
      return   
         
         
      
      
      
      
