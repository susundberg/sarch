import unittest
from unittest.mock import patch, MagicMock
import time
import subprocess

from .common import RepoInDir, LogOutput
from threading import Thread
class TestSSHRepo(unittest.TestCase):
   
   def setUp(self):
      
      self.log = LogOutput( self.assertEqual )
      self.log.start()
      
      self.repo_local  = RepoInDir("repo_local", self.assertEqual )
      self.repo_remote = RepoInDir("repo_remote", self.assertEqual )
      self.repo_local.fillup_std_layout()
      
      # Start new thread
      #
      
      # Now we can execute sync to that server, we just need to fool the stdout
   def test_invalid_url(self):
      self.repo_local.main("sync","ssh://foobar", assumed_ret=-1)
   
   def open_ssh_connection(self):
     self.server = subprocess.Popen(["python3", "-m", "sarch", "_server_mode", self.repo_remote.test_dir], stdout=subprocess.PIPE, stdin=subprocess.PIPE, stderr=subprocess.PIPE, close_fds=True )
     self.patched_popen = MagicMock( return_value=self.server)
     
   def close_ssh_connection(self):
      self.server.wait(1)
      self.assertEqual( 0, self.server.returncode )
      self.server.stdout.close()
      self.server.stdin.close()
      self.server.stderr.close()
      
   
   def test_sync(self, assumed_ret = 0):
     self.open_ssh_connection()         
     with patch("sarch.remote_ssh.Popen", self.patched_popen ) as mocked:
        self.repo_local.main("sync","ssh://dontcare.com:/foo/bar", assumed_ret=assumed_ret)
     self.close_ssh_connection()

     if assumed_ret == 0:
        self.repo_local.check_equal( self.repo_remote )
     
   def test_sync_after_mods( self ):
     self.test_sync()
     self.repo_local.make_std_mods() 
     self.test_sync()
     self.repo_local.check_equal( self.repo_remote )    
     
   def test_sync_after_mods_another( self ):
     self.test_sync()
     self.repo_remote.make_std_mods() 
     self.test_sync()
     self.repo_local.check_equal( self.repo_remote )    
   
   def test_sync_when_staging_on_server( self ):
     self.repo_remote.file_make("FOO")
     self.repo_remote.main("add", "FOO" )
     self.test_sync(assumed_ret=-1)
     
   


