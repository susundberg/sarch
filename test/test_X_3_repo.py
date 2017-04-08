import unittest



from .common import RepoInDir, LogOutput



class Test3LocalRepo(unittest.TestCase):
   
   
   def setUp(self):
      
      self.log = LogOutput( self.assertEqual )
      self.log.start()
      
      self.repo_a = RepoInDir("repo_a", self.assertEqual )
      self.repo_b = RepoInDir("repo_b", self.assertEqual )
      self.repo_c = RepoInDir("repo_c", self.assertEqual )
      self.repos = ( self.repo_a, self.repo_b, self.repo_c )
      self.repo_a.fillup_std_layout()
   
   def make_3_way_sync(self):
      
      self.repo_a.sync( self.repo_b )
      self.repo_b.sync( self.repo_c )
      self.repo_c.sync( self.repo_a )
      self.repo_c.check_equal( self.repo_a )
      self.repo_b.check_equal( self.repo_a )
      
   def test_clean_status(self):
      for repo in reversed( self.repos ):
         repo.main("status")
      self.log.info_contains( "0 files", 2 )
      self.make_3_way_sync()
      for repo in reversed( self.repos ):
         repo.main("status")
   
   def test_status_after_mod(self):
      self.make_3_way_sync()
      self.repo_a.make_std_mods()
      self.make_3_way_sync()
      
         
   def tearDown(self):
      for repo in reversed( self.repos ):
         repo.clean()
      self.log.stop()
  
   