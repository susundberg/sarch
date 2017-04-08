import unittest
import os

from .common import TestBase


class TestCommit( TestBase ):
   
    
    def test_commit_no_changes( self ):
       self.repo.main( "commit",  )
    
    def test_commit_auto( self ):
       self.repo.fs.move("FOO","FOO_MOVED")
       self.repo.main( "add", "FOO_MOVED" )
       self.repo.file_make("BAR", timestamp=2**20 )
       self.log.clear()
       self.repo.main( "commit", "-a","-m","My first autocommit!" )
       self.repo.main( "status" )
       
    def test_commit_sub( self ):
       self.repo.db_check_size( 0,0,0)
       
       os.chdir( self.repo.fs.make_absolute( "dir1/dir2"  ) )
       self.repo.main_set_cwd( False )
       self.repo.main( "add", "." )
       self.repo.main( "commit",  )
       
       self.repo.db_check_size( 0,0,0)
       
       self.repo.file_make_many( ("NEW_BAR1","NEW_FOO1"), basepath=("dir1","dir2") )

       self.repo.main( "add", "." )
       self.repo.main( "commit",  )
       self.repo.db_check_size( 1,2,0 )
       
       os.chdir( self.repo.test_dir )
       
       self.repo.main( "rm", "dir1",  )
       self.repo.main( "commit",  )
       self.repo.db_check_size( 2,2,0)
       
