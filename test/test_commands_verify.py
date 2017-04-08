
import os

from .common import TestBase

class TestVerify( TestBase ):
   
    def test_verify_all(self):
       self.repo.main( "verify", )
       # Then modify a file
       self.repo.file_make( "FOO", timestamp = 2**30, content="MODIFIED" )
       
       self.repo.main( "verify", assumed_ret = 1 )
       
       # Commit without adding does nothign
       self.repo.main( "commit", )
       self.repo.main( "verify", assumed_ret = 1 )
       
       # But after its updated all should be good.
       self.repo.main( "add","FOO" )
       self.repo.main( "commit",  )
       self.repo.main( "verify", )
       
    def test_verify_single(self):
       self.repo.main( "verify", "FOO" )
       self.repo.main( "verify", "FOO", "BAR" )
       
       
    def test_verify_nonext(self):
       self.repo.main( "verify",  "XXX" , assumed_ret = -1)
       self.repo.main( "verify",  "XXX","YYY" , assumed_ret = -1)
       self.repo.file_make( "XXX" )
       self.repo.main( "verify",  "XXX" , assumed_ret = -1 )