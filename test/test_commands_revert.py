
from .common import TestBase
from sarch.database import Meta
from pathlib import Path

class TestRevert( TestBase ):
    
    def test_revert(self) -> None :
       self.repo.main( "revert", )
    
    
    def test_revert_revert_auto_commit(self) -> None:
       self.repo.file_make("FOO", timestamp=2**10)
       self.repo.main( "revert", "FOO" ) 
       self.repo.main( "revert", "FOO" ) 
       self.repo.main("status", assumed_ret=1)
       self.repo.main("commit","-a")
       self.repo.commit_check_log()
    
    def test_revert_revert(self) -> None:
       self.repo.file_make("FOO", timestamp=2**10)
       self.repo.main( "revert", "FOO" ) 
       self.repo.db_is_reverted( "FOO", True) 
       self.repo.main( "revert", "FOO" ) 
       self.repo.db_is_reverted( "FOO", False) 
       self.repo.main("status", assumed_ret=1)
       self.repo.main("add", "FOO")
       self.repo.main("commit")
       self.repo.commit_check_log()
       
       
    def test_revert_add(self) -> None :
       self.repo.file_make( "NEW_FOO", )
       self.repo.main( "add", "NEW_FOO"  )
       self.repo.main( "revert", )
       self.repo.main( "revert", "FOO" ) #Nothing to revert
       self.repo.db_is_reverted( "FOO", False )
       
    def test_revert_with_names(self) -> None :
       self.repo.main( "rm", "dir1"  )
       self.repo.main( "rm", "FOO", "BAR"  )
       check_foo = Path("dir1", "dir2", "FOO" )
       
       self.repo.file_check( check_foo, exists = False )
       self.repo.file_check( "FOO", exists = False )
       self.repo.file_check( "BAR", exists = False )
       
       self.repo.main( "revert", "dir1", "FOO" )
       self.repo.file_check( check_foo, exists = True )
       self.repo.file_check( "FOO", exists = True )
       self.repo.file_check( "BAR", exists = False )
       self.repo.db_is_reverted( "FOO", False ) 
       
    def test_revert_rm_really( self ) -> None:
       self.repo.file_del("FOO")
       self.repo.file_check( "FOO", exists = False )
       self.repo.main( "rm","FOO"  )
       self.repo.main( "revert" ,  )
       self.repo.file_check( "FOO", exists = False )
       self.repo.db_is_reverted( "FOO", True )
    
    def test_revert_modified_staging( self ) -> None:
       self.repo.file_make("FOO", timestamp=2**10)
       self.repo.main("add", "FOO" )
       self.repo.main( "revert" , )
       self.repo.db_is_reverted( "FOO", True )
       
    def test_revert_modified_not_staging( self ) -> None:
       self.repo.file_make("FOO", timestamp=2**10)
       self.repo.file_del("BAR")
       self.repo.main( "revert" , "FOO", "BAR"  )
       self.repo.db_is_reverted( "FOO", True )
       self.repo.db_is_reverted( "BAR", True )
       
       
    def test_revert_rm( self ) -> None:
      self.repo.main( "rm", "dir1"  )
      check_foo = Path("dir1", "dir2", "FOO" )
      check_bar = Path("dir1", "dir2", "BAR" )
      self.repo.file_check( check_foo, exists = False )
      self.repo.main( "revert"  )
      self.repo.file_check( check_foo, exists = True)
      self.repo.file_check( check_bar, exists = True )
