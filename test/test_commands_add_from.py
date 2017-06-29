import os

from .common import TestBase,TempDir
from sarch.database import Meta

class TestAddingFrom( TestBase ):
   
   def setUp(self) -> None:
      super().setUp()
      self.other =  TempDir( self.assertEqual )
   
   def basic_init(self):
      self.other.file_make_many( ["FILE1", "FILE2"], timestamp=2**20 )
      self.other.file_make_many( ["FILE3", "FILE4"], timestamp=2**20 + 60*60*24*60 )
      self.other.file_make_many( ["FILE1", "FILE2"], basepath=["FOO","BAR"], timestamp=2**20 + 60*60*24*60*2 )
      
   def basic_test(self, tdir ) -> None:
      
      os.chdir( self.repo.test_dir + "/" + tdir )
      self.repo.main("add_from", self.other.test_dir, no_cd = True )
      self.repo.main("commit")
      self.repo.commit_check_log()
      
   def basic_check(self,tdir):
      self.repo.file_check( tdir + "1970-05/FILE1", exists=True )
      self.repo.file_check( tdir + "1970-01/FILE1", exists=True )

   def test_basic_subdir( self ) -> None:
      self.basic_init()
      self.basic_test("dir1/")
      self.basic_check("dir1/")
      
   def test_basic_rootdir( self ) -> None :
      self.basic_init()
      self.basic_test("")
      self.basic_check("")
   
   def test_overwrite_differ( self ):
      for loop in range(4):
         self.other.file_make( "FOO", subs=("dir%d" % loop ) )
      self.basic_test("")
      for loop in range(3):
        self.repo.file_check( "1970-01/FOO-%03d" % loop, exists=True )
      
   def test_overwrite_identical( self ):
      meta = Meta( "FOO" )
      self.repo.fs.meta_update(meta)
      for loop in range(4):
         meta_new = meta.copy()
         data = self.repo.fs.file_read( meta.filename )
         meta_new.filename = "dir%d/FOO" % loop
         self.other.fs.file_create( meta_new, data )
      self.basic_test("dir1")
      self.repo.file_check( "dir1/1970-01/FOO", exists=True )
      self.repo.file_check( "dir1/1970-01/FOO-000", exists=False )
      
                   
      