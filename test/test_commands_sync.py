
import os
import shutil
from unittest.mock import patch

from .common import TestBase, RepoInDir
from sarch.remote_localfs import RemoteLocalFS
from sarch.database import Meta, DatabaseBase
from sarch.remote import SA_SYNC_Exception
from sarch.filesystem import Filesystem


class SyncBase( TestBase ):
   def setUp(self) -> None:
      super().setUp()
      self.other = RepoInDir( "other", self.assertEqual )
      self.do_sync()
      
   def tearDown(self) -> None:   
      self.other.clean()
      super().tearDown()
      
   def do_sync(self, verbose = False, assumed_ret = 0 ) -> None:
      self.repo.sync( self.other, assumed_ret=assumed_ret )
      self.other.sync( self.repo, assumed_ret=assumed_ret )
      
      if assumed_ret == 0:
         self.repo.check_equal( self.other )
   

class TestSyncCancelled( SyncBase ):
   def setUp(self) -> None:
      super().setUp()
      # Lets create some files
      self.filenames = [ "FILE%03d" % loop for loop in range(16)]
      self.repo.file_make_many( self.filenames  )
      self.repo.main("add", *self.filenames)
      self.repo.main("commit")
      
      original_file_set = RemoteLocalFS.file_set
      
      def spoofed_file_set( self, target : Meta, *pargs, **kwargs ):
         if target.filename == "FILE008":
            raise SA_SYNC_Exception("THIS IS TEST EXCEPTION")
         return original_file_set( self, target, *pargs, **kwargs )
      
      self.spoofed_set = spoofed_file_set
      self.check_db_status_is(DatabaseBase.STATUS_CLEAR)
      
      with patch.object(RemoteLocalFS, 'file_set', new=self.spoofed_set): # type: ignore
         self.repo.sync( self.other, assumed_ret = -1 )
      
      self.check_db_status_is(DatabaseBase.STATUS_SYNC)
      
   def check_db_status_is( self, status ):
      for repo in [self.repo, self.other]:
         repo.open_db()
         self.assertEqual( status , repo.db.get_status()   )
   
   def test_next_commit_ok(self):
        self.repo.sync( self.other, assumed_ret = 0 )   
        self.check_db_status_is(DatabaseBase.STATUS_CLEAR)
        self.do_sync() 

   def _make_file08(self):
           self.other.file_make("FILE008", content="FILE TRANSFERRED; BUT GOT CANCELLED WHILE RENAMING")
           self.other.fs.trash_add("FILE008")
           self.other.file_make("FILE008", content="PARTIAL_FILE")
              
   def test_partial_on_disk_trash_gets_cleared(self):
        # First try with the file beeing on the trash only (should be gone on start)
        self._make_file08(  )
        self.repo.sync( self.other, assumed_ret = -1 )   
        
   def test_partial_on_disk( self ):
        # Then try it with 
        self._make_file08( )
        with patch.object(Filesystem, 'trash_clear'): # type: ignore
           self.repo.sync( self.other, assumed_ret = 0 )   
        
   
class TestSync( SyncBase ):
   
      
   def _add_n_commit_new_file(self):
      self.repo.main( "add", "NEW_FILE" ) 
      self.repo.main( "commit", )
   
   def test_sync_local_exists_set( self ) -> None:
      self.other.file_make("NEW_FILE")
      self.repo.file_make( "NEW_FILE", )
      self._add_n_commit_new_file()
      self.do_sync(assumed_ret = -1)
      
   def test_sync_local_exists_copy( self ) -> None:
      self.other.file_make("NEW_FILE")
      self.repo.file_copy("FOO", "NEW_FILE")
      self._add_n_commit_new_file()
      self.do_sync(assumed_ret = -1)

      
   def _create_deleted_and_commit( self ) -> None:
      self.repo.main( "rm", "FOO" )
      self.repo.main( "commit" )
      self.do_sync()
       
      self.other.file_make( "FOO", content="THIS MAY NOT BE OVERWRITTEN, NOT COMMITED" )
      self.other.main( "add", "FOO" )
      self.other.main( "commit" )
      
   
   def test_sync_with_overwrite_new_on_top_of_deleted( self ) -> None:
      self._create_deleted_and_commit()
      self.do_sync()
   
   def test_sync_with_identical_files( self ) -> None:
      def make_a_identical_files( repo ):
         fns = []
         content = ""
         for loop in range(16):
            content = content + "FOO FOO FOO" * loop
            fn = "IDFILE%03d" % loop
            fns.append(fn)
            repo.file_make( fn, content=content, timestamp=2**10 + loop )
         repo.main("add", *fns )
         repo.main("commit")
         
      make_a_identical_files( self.repo )
      make_a_identical_files( self.other )
      self.log.clear()
      self.do_sync()
      self.log.info_contains( ": merge identical", 16 )
      
   def test_sync_with_overwrite_new_on_top_of_deleted_with_ut( self ) -> None:
      self._create_deleted_and_commit()
      self.repo.file_make( "FOO", content="THIS IS ANOTHER IMPORTANT FILE, IT MUST NOT BE OVERWRITTEN" )
      self.do_sync( assumed_ret = -1)
      
   def test_sync_with_revert_reverted( self ) -> None:
      self.repo.file_make( "FOO", timestamp = 2**20, content="OH, I ACCIDENTALLY MODIFIED THIS FILE" )
      self.repo.main( "revert", "FOO" )
      self.repo.db_is_reverted("FOO", True)
      self.repo.main( "revert", "FOO" )
      self.repo.db_is_reverted("FOO", False)
      self.repo.main( "status", assumed_ret = 1)
      self.do_sync( assumed_ret = -1 )
      
   def test_sync_with_revert( self ) -> None:

      self.repo.file_make( "NEW_FILE", )
      self._add_n_commit_new_file()
      cs_before = self.repo.db_get("FOO").checksum
      self.repo.file_make( "FOO", timestamp = 2**20, content="OH, I ACCIDENTALLY MODIFIED THIS FILE" )
      self.repo.file_del("BAR")
      self.repo.main( "revert", "FOO", "BAR" )
      self.repo.db_is_reverted("FOO", True)
      self.repo.db_is_reverted("BAR", True)
      self.do_sync( )
      
      cs_now = self.repo.db_get("FOO").checksum
      self.assertEqual( cs_now, cs_before )
      self.repo.main("verify")
      self.repo.db_is_reverted("BAR", False)
      
   def test_sync_with_revert_and_modified( self ) -> None:
      self.repo.file_make( "FOO", timestamp = 2**20, content="OH, I ACCIDENTALLY MODIFIED THIS FILE" )
      self.repo.main("revert", "FOO" )
      self.other.file_make( "FOO", timestamp = 2**21, content="THIS IS REAL MODIFICATION" )
      self.other.main("commit", "-a" )
      cs_real = self.other.db_get("FOO").checksum
      self.do_sync( )
      cs_now = self.repo.db_get("FOO").checksum
      self.assertEqual( cs_now, cs_real )
   
   def test_sync_both_reverted( self ) -> None:
      for repo in [self.repo, self.other]:
         repo.file_make( "FOO", timestamp = 2**21, content="REVERT ME" )
         repo.main("revert", "FOO" )
      self.do_sync( )
      self.repo.db_is_reverted("FOO", True)
      self.other.db_is_reverted("FOO", True)
         
         
   def test_sync_when_dirty_staging( self ) -> None:
      self.repo.file_make( "NEW_FILE", )
      self.repo.main( "add", "NEW_FILE" ) 
      self.do_sync( assumed_ret = -1)
   
   def test_sync_when_nothing(self)->None:
      self.do_sync( )
      self.do_sync( )
      
   def test_sync_when_dirty_modified( self ) -> None:
      # Make a update that needs to be uploaded
      self.repo.file_make( "FOO", timestamp = 2**20, content="INVALID")
      self.repo.main( "add", "FOO" )
      self.repo.main( "commit", )
      self.repo.main( "status", ) # Now the main repo has modification to be synced
      
      self.repo.file_make( "FOO", timestamp = 2**21 ) # And now it has untracked modification
      self.repo.main( "status", assumed_ret = 1 )
      self.do_sync(  assumed_ret = -1)
   
   def test_sync_after_move( self ) -> None:
      self.repo.fs.move("FOO", "FOO_MOVED")
      self.repo.main( "add", "FOO_MOVED")
      self.repo.main( "commit","--auto")
      self.log.clear()
      self.do_sync()
      self.log.info_contains( "#SYNC:FOO_MOVED: move from FOO",1 )

                     
   def test_sync_after_add( self ) -> None:
      new_files = [] # type: List[str]
      new_files += self.repo.file_make_many( ("NEW_FILE", "FOO_COPY",) )
      new_files += self.repo.file_make_many( ("FILE",), basepath=("NEW_DIR", "NEW_DIR2" ))
      
      # And copy a existing file
      shutil.copyfile( self.repo.fs.make_absolute("FOO"), self.repo.fs.make_absolute("FOO_COPY") )
      
      self.repo.main( "add", *new_files )
      self.repo.main( "commit", )
      self.log.clear()
      self.do_sync()
      self.log.info_contains( "#SYNC:FOO_COPY: copy_local from FOO",1 )
      self.log.info_contains( "#SYNC:NEW_DIR/NEW_DIR2/FILE: copy new", 1)
   
   def test_sync_after_mod_del( self ) -> None:
      self.repo.file_del("FOO")
      self.do_sync( )
      
   def test_sync_after_del( self ) -> None:
      self.repo.main( "rm", "FOO")
      self.repo.main( "commit", ) 
      self.repo.file_check("FOO", exists=False)
      self.do_sync( )
      self.repo.file_check("FOO", exists=False)
      self.other.file_check( "FOO", exists=False)
   
     
   def create_conflict( self ) -> None:
      def make_conflict( repo ) -> None:
         for loop in range(3):
            fn = "CONFLICTFILE%03d" % loop
            repo.file_make( fn, )
            repo.main( "add", fn ) 
            repo.main( "commit", ) 
      make_conflict( self.repo )
      make_conflict( self.other )
      
   def patch_input_and_test( self, returns, testfun ) -> None:
      with patch('sarch.remote.read_input') as patched_input:
         patched_input.side_effect = returns
         testfun(self)
         
   def test_conflict_local(self) -> None:
      self.create_conflict()
      def patched_test(self) -> None:
         self.do_sync( )
      self.patch_input_and_test( ("l","o","l"), patched_test )      
      
   def test_conflict_bailout(self) -> None:
      self.create_conflict()
      def patched_test2(self) -> None:
         target = "file://" + self.other.test_dir 
         self.repo.main( "sync", target, "--verbose", assumed_ret = -1 )
      self.patch_input_and_test( ("x","D",""), patched_test2 )      
   
   def test_sync_invalid_target(self) -> None:
      self.repo.main( "sync", "/tmp/nonexisting", "--verbose", assumed_ret = -1 )
      self.repo.main( "sync", "/tmp/", "--verbose", assumed_ret = -1 )
      self.repo.main( "sync", "invalid_proto:///tmp/nonexisting", "--verbose", assumed_ret = -1 )
      


