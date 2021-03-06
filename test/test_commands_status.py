
from .common import TestBase

from pathlib import Path
import os

class TestStatus( TestBase ):
   
    def test_status_ok(self):
       self.repo.main("status" )
       self.log.info_contains( "6 Files - all good" )
       
    def test_status_subdir(self):
       os.chdir( str( Path(self.repo.test_dir) / Path("dir1/dir2")) )
       self.repo.main("status" , no_cd=True)
       self.log.info_contains( "2 Files - all good" )

    def test_status_mod(self):   
       # Then modify a file
       self.repo.file_make("FOO", timestamp=2**10)
       self.repo.main("status", assumed_ret = 1 )
    
       
    def test_status_added(self):      
       self.repo.file_make("NEW_FOO")
       self.repo.main("status", assumed_ret = 1 )
       # But after its updated all should be good.
       self.repo.main("add","NEW_FOO" )
       self.repo.main("rm","FOO" )
       self.repo.main("status", )
       self.repo.main("commit", )
       self.repo.main("status")
       
    def test_status_removed_created(self):
       self.repo.file_make( "REMOVED", )
       self.repo.main("status", assumed_ret = 1 )
       
    def test_status_removed(self):
       self.repo.file_del("FOO")
       self.repo.main("status", assumed_ret = 1 )
    
    def test_status_reverted(self):  
       self.repo.main("status")
       self.repo.file_make("FOO", timestamp=2**10)
       self.repo.main("status", assumed_ret = 1 )
       self.repo.main("revert","FOO")
       self.repo.main("status", assumed_ret = 1 )
       