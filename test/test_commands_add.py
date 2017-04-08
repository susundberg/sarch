import os

from .common import TestBase


class TestAdding( TestBase ):
   
    def test_add_relative(self):
       
       self.repo.file_make_many( ("NEW_BAR","NEW_FOO"),  )
       self.repo.file_make_many( ("NEW_BAR1","NEW_FOO1"), basepath=("sub1",) )
       
       os.chdir( self.repo.fs.make_absolute( "." ) )
       self.repo.main( "add","./NEW_FOO" )
       self.repo.main( "add","sub1/NEW_FOO1"  )
       

       os.chdir( self.repo.fs.make_absolute( "sub1" ) )
       self.repo.main( "add","NEW_BAR1" , no_cd=True )
       self.repo.main( "add","../NEW_BAR", no_cd=True  )
       
    
    def test_add_nonext( self ):
       self.repo.main( "add", "NONEXT" , assumed_ret=-1 )
    
    def test_add_op_pending( self ):
       self.repo.main( "rm", "FOO" )
       self.repo.file_make( "FOO", timestamp=2**20 )
       self.repo.main( "add", "FOO", assumed_ret = 1 )
       
    def test_add_op_revert( self ):
       self.repo.file_make( "FOO", timestamp=2**20 )
       self.repo.main( "revert", "FOO")
       self.repo.main( "add", "FOO", assumed_ret = 1 )

