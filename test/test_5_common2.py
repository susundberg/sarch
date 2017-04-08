
from sarch.common import CONFIG

from .common import TestBase
import shutil
from os.path import join
class TestMain( TestBase ):
   
    def test_base(self):
       self.repo.main("status")
       self.repo.commit_check_log()

    def test_invalid(self):
       self.repo.main("XXX_invalid", assumed_ret=-1)
       
    def test_help(self):
       self.repo.main("help")
       self.repo.main("help","add")
       self.repo.main("help","XXX_invalid", assumed_ret=-1)

    def test_no_repo(self):
       shutil.rmtree( join( self.repo.test_dir, CONFIG.PATH ) )
       self.repo.main("add","FOO", assumed_ret=-1)
       