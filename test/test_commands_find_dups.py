from .common import TestBase
from sarch.database import Meta

class TestFindDups( TestBase ):
   
   def make_identical( self ) -> None:
      meta = Meta( "FOO" )
      self.repo.fs.meta_update(meta)
      fns = []
      for loop in range(4):
         meta_new = meta.copy()
         data = self.repo.fs.file_read( meta.filename )
         meta_new.filename = "FOO_%03d" % loop
         fns.append( meta_new.filename )
         self.repo.fs.file_create( meta_new, data )
      self.repo.main("add", *fns)   
      self.repo.main("commit")
      
   def test_basic(self):
      self.log.set_verbose(True)
      self.log.clear()
      self.repo.main("find_dups")
      self.log.info_contains( "No duplicate" )
      self.make_identical()
      self.log.clear()
      self.repo.main("find_dups")
      self.log.info_contains( "duplicate files" )
      self.log.info_contains( "FOO_002" )
