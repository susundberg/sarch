

from .common import TestBase

class TestLog( TestBase  ):
    
    def test_log(self):
       self.repo.main( "log",  )
       
    def test_log_dir(self):
       self.repo.main( "log", "dir1",  )

    def test_log_dir_nonext(self):
       self.repo.main( "log", "XXX",  )
    
    def test_log_removed( self ):
       self.repo.main( "log", "REMOVED",  )
       
    def test_log_single(self):

       for ncommits in range(32):
         self.repo.file_make("FOO", content=("MODIFIED %08d" % ncommits*11 ), timestamp=(100000 + ncommits*1000 ))
         self.repo.main( "add", "FOO"  )
         self.repo.main( "commit", )

       self.log.clear()
       self.repo.main( "log", "FOO"  )
       self.assertTrue( len(self.log.info) > 10 )
       
       self.log.clear()
       self.repo.main( "log", "FOO", "--count", "3"  )
       self.assertTrue( len(self.log.info) < 6 )


