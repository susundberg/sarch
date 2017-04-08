from .common import TestBase
from pathlib import Path

from unittest.mock import patch, MagicMock
from io import StringIO

class TestServerMode( TestBase  ):
    
    def test_server_start(self):
       
      comm_in_file = Path(self.repo.test_dir, "COMM_IN" )
      comm_out_file = Path(self.repo.test_dir, "COMM_OUT" )
      
      fid = open( str(comm_in_file ), 'wb' )
      fid.write(bytes('{"cmd": "hello", "par": ["1.0.0"]}\0{"cmd": "close", "par": []}\0',"utf8"))
      fid.close()
      
      fid_in = open( str(comm_in_file ), 'r' )
      fid_out = open( str(comm_out_file ), 'w' )
         
      with patch("sys.stdin", fid_in), patch("sys.stdout", fid_out ):
         self.repo.main( "_server_mode", self.repo.test_dir )
      
      fid_in.close()
      fid_out.close()
   
    def test_server_with_mod( self ):
      self.repo.file_make("FOO", timestamp=2**10)
      with patch("sys.stderr", MagicMock()):
         self.repo.main( "_server_mode", self.repo.test_dir, assumed_ret = -1 )

