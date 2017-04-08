
import os
import shutil
from os.path import join
from .common import TestBase


class TestRm( TestBase ):

    def test_rm_file(self) -> None:
       self.repo.file_check( "FOO", exists = True )
       self.repo.main( "rm", "FOO" )
       self.repo.file_check( "FOO", exists = False )
       self.repo.db_check_size( 0, 0, 1 )
       self.repo.commit_check_log()

    def test_rm_deleted_file(self) -> None:
       self.repo.file_del("FOO")
       self.repo.file_del("BAR")
       self.repo.main( "rm", join( self.repo.test_dir, "FOO") , "BAR"  )
       self.repo.file_check( "FOO", exists = False )
       self.repo.db_check_size( 0, 0, 2 )
       self.repo.commit_check_log(with_output = True)

    def test_rm_deleted_path(self) -> None:
       # Make sure there is file to be removed
       self.repo.main( "rm", join("dir1","dir2","BAR")  )
       self.repo.db_check_size( 0, 0, 1 )
       self.repo.commit_check_log()
       self.repo.db_check_size( 1, 0, 0 )
       
       # Then remove the full path
       os.chdir( self.repo.fs.make_absolute("dir1") )
       shutil.rmtree( "dir2" )
       self.repo.main( "rm", "dir2", no_cd = True )
       os.chdir( self.repo.test_dir )
       self.repo.db_check_size( 1, 0, 1 )
       self.repo.commit_check_log(with_output = True)
       self.repo.file_check( "dir1", exists = False )
       
    def test_rm_added_file(self) -> None:
       self.repo.file_make( "EXTRA_FILE", subs = ["dir1" ] )
       self.repo.main( "add", "dir1" )
       self.repo.main( "rm", "dir1", assumed_ret = 1  )
       
    def test_rm_path(self) -> None:
       self.repo.file_make( "EXTRA_FILE", subs = ["dir1" ] )
       self.repo.main( "add", "dir1" )
       self.repo.main( "commit" )
       
       self.repo.main( "rm", "dir1" )
       self.repo.file_check( "dir1", exists = False )
       self.repo.db_check_size( 1, 1, 3 )
       self.repo.commit_check_log()
       
    def test_rm_path_with_extra_file(self) -> None:
       self.repo.file_make( "EXTRA_FILE", subs = ["dir1", "dir2" ] )
       self.repo.file_check( "dir1/dir2/EXTRA_FILE", exists = True)
       
       self.repo.main( "rm", "dir1" )
       self.repo.file_check( "dir1", exists = True )
       self.repo.file_check( "dir1/dir2/FOO", exists = False)
       self.repo.file_check( "dir1/dir2/EXTRA_FILE", exists = True)
       self.repo.db_check_size( 0, 0, 2 )
       

    def test_rm_nonexit(self) -> None:
       self.repo.main( "rm", "NONEXT", assumed_ret = -1 )
       self.repo.main( "rm", "REMOVED", assumed_ret = -1)
       self.repo.db_check_size( 0, 0, 0 )
       self.repo.commit_check_log()
       