from pathlib import Path
import os
import shutil
import tempfile


from sarch.common import CONFIG
from sarch.database_json import DatabaseJson
from sarch.database import Meta
from sarch.filesystem import Filesystem
from sarch.__main__ import main as sarch_main_raw

import sarch.common

import json

CONFIG.DATA_BLOCK_SIZE = 4

class LogOutput:
   
   def __init__(self, check):
      self.clear()
      self.check = check
      self.verbose = False
      
   def clear( self ):
      self.info = []
      self.error = []
   
   def fun_info( self, string ):
      self.info.append( string )
      if self.verbose:
         print("II:" + string )
   
   def fun_error( self, string, file ):
      self.error.append( string )
      if self.verbose:
         print("EE:" + string )
   
   def info_contains( self, string, count = 1 ):
      string = string.lower()
      found = 0 
      for item in self.info:
        if string in item.lower():
           found += 1
      self.check( count, found )
        
   def start( self ):
      self.orig_info  = sarch.common.output
      sarch.common.output = self.fun_info
      self.orig_error = sarch.common.output_error
      sarch.common.output_error = self.fun_error
      
   
   def stop( self ):
      sarch.common.output = self.orig_info
      sarch.common.output_error = self.orig_error
   
   def set_verbose( self, status : bool = True ):
      self.verbose = status
   
class TempDir:
   
   def __init__(self, check ) -> None:
      self.start_dir  = os.getcwd()
      self.test_dir   = tempfile.mkdtemp()
      self.fs = Filesystem( self.test_dir )
      self.check = check
      
   def clean(self) -> None:   
      os.chdir( self.start_dir )
      shutil.rmtree(self.test_dir)
   
   def file_del ( self, filename ):
      fn = Path( self.fs.make_absolute( filename ) )
      os.unlink( str(fn) )
      
   def file_check( self, filename, exists, checksum = None ):
      fn = Path( self.fs.make_absolute( filename ) )
      self.check( exists, fn.exists() )
      if checksum != None:
         meta = Meta( filename )
         self.fs.meta_update( meta )
         self.check( meta.checksum, checksum )
    
   def file_copy( self, filename_source :str, filename_target:str ) -> None:
      fid = self.fs.file_read( filename_source )
      meta = Meta( filename_target )
      self.fs.file_create( meta, fid )
      
      
   def file_make( self, filename :str , content : str = None , timestamp :int = (2**20 + 3145), subs = (".",) ) -> str:
      basepath  = Path(*subs) 
      filename = str( basepath / Path(filename) )
      
      absname_path = self.fs._make_absolute( filename )
      self.fs.make_directories( absname_path.parent )
      absname = str(absname_path)
      
      if content == None:
         content = ( absname ) * 8
         
      with open( absname, 'wb' ) as fid:
         fid.write( bytes(content,"utf8") )
         
      self.fs._file_set_modtime( absname, timestamp )
      return filename
       
   def file_make_many( self, filenames, basepath = (".",), content = None , timestamp = 2**20 + 3145 ):
       created = []
       for fn in filenames:
          fn = self.file_make( fn, content=content, timestamp=timestamp, subs=basepath )
          created.append( fn )
       return created
    
   
class RepoInDir(TempDir):
   def __init__( self, name : str, assertfun ) -> None:
      super().__init__( assertfun )
      self.no_cd = None # type: bool
      self.name = name
      self.check = assertfun
      self.main( "init", self.name )
      self.fs = Filesystem( self.test_dir )
      self.db = DatabaseJson( )
      self.db_sizes = (0,0,0)

   def open_db( self ):
       self.db.open_from_path( self.fs.make_absolute( CONFIG.PATH ) )

   def commit_check_log( self, with_output = False ):
       self.main( "commit" )
       self.main( "verify" )
       self.main( "status"  )
       self.main( "log"  )
   
   def db_get( self, filename ) -> Meta:
      self.open_db()
      return self.db.meta_get( filename )
      
   def db_update_size(self):
      self.open_db()
      self.db_sizes = self.db.get_table_sizes()
   
   def db_is_reverted( self, filename, status ):
      self.check( status, Meta.CHECKSUM_REVERTED == self.db_get(filename).checksum )
       
   def db_check_size(self, n_commits, n_stor, n_staging, absolute=False ):
       self.open_db()
       lens = self.db.get_table_sizes()
       add_sizes = self.db_sizes
       if absolute == True:
          add_sizes = (0,0,0,)
          
       self.check( add_sizes[0] + n_commits, lens[0] )
       self.check( add_sizes[1] + n_stor, lens[1] )
       self.check( add_sizes[2] + n_staging, lens[2] )


   def main_set_cwd( self, no_cd ):
      self.no_cd = no_cd
      
   def main( self, *arguments, assumed_ret = 0, verbose = False, no_cd=None):
      
      if no_cd == None:
        if self.no_cd != None:
           no_cd = self.no_cd
        else:
           no_cd = False
           
      if no_cd == False:    
         os.chdir( self.test_dir )
         
      ret = sarch_main_raw( arguments )
      self.check( ret, assumed_ret )
      
      if no_cd == False:
         os.chdir( self.start_dir )
      
   def sync( self, other : 'RepoInDir', assumed_ret:int = 0 ):
      target = "file:///%s" % other.test_dir
      self.main("sync", target, "--verbose", assumed_ret=assumed_ret )
      
   def check_equal( self, other : 'RepoInDir'):
      fn_local = { x for x in self.fs.recursive_walk_files(".") }
      fn_remote = { x for x in self.fs.recursive_walk_files(".") }
      self.check( fn_local, fn_remote )
      
      self.open_db()
      other.open_db()
      
      # Sets are equal -> all files ok
      for fn in fn_local:
         meta_local = Meta(fn)
         meta_other = Meta(fn)
         self.fs.meta_update( meta_local )
         other.fs.meta_update( meta_other )
         
         self.check( meta_local.modtime, meta_other.modtime )
         self.check( meta_local.checksum, meta_other.checksum )
         
      self.check_databases( other )
      other.check_databases( self )
      
   def check_databases( self, other : 'RepoInDir' ):
      for key in ["stag", "commit", "stor"]:
         values_local = self.db.db[key]
         values_other = other.db.db[key] 
         for (key,value) in values_local.items():
            self.check( value, values_other[key] )
   
      
   def make_std_mods( self ):
      self.file_make( "FOO", timestamp=(2**10) )
      self.main("add", "FOO" )
      self.main("commit")
      self.fs.move( "FOO","FOO_NEW"  )
      self.main("add", "FOO_NEW" )
      self.main("rm", "FOO" )
      self.main("commit")
      self.main("status")
      self.file_copy( "BAR","BAR_COPY"  )
      self.main("add", "BAR_COPY" )
      self.file_make( "FOO_NEW", timestamp=(2**12) )
      self.main("add", "FOO_NEW" )
      self.commit_check_log()
      
   def fillup_std_layout( self ):
       files = []
       files += self.file_make_many( ["FOO", "BAR","REMOVED","REMOVED_ADDED"] )
       files += self.file_make_many( ["FOO", "BAR" ], basepath = [ "dir1", "dir2" ] )
       files += self.file_make_many( ["FOO",  ], basepath = ["sdir1", "sdir2" ] )
       self.main( "add", *files )
       self.main( "commit", "--msg", "Initial commit" )
       self.main( "rm", "REMOVED", "REMOVED_ADDED")
       self.main( "commit",  )
       self.file_make("REMOVED_ADDED", timestamp = 2**20 + 3144 )
       self.main( "add", "REMOVED_ADDED" )
       self.main( "commit",  )


import unittest

class TestBase(unittest.TestCase):
     
    def setUp(self):
       self.repo = RepoInDir("testrepo", self.assertEqual )
       self.log  = LogOutput( self.assertEqual )
       self.log.start()
       self.repo.fillup_std_layout()
       self.repo.db_update_size()
       
    def tearDown(self):
       self.repo.clean()
       self.log.stop()
      
