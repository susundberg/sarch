

import unittest
import os
from os.path import join

from sarch.filesystem import Filesystem, SA_FS_Exception_NotFound

from sarch.database import Meta
from sarch.common import CONFIG
from .common import TempDir, LogOutput


class TestFS( unittest.TestCase ):
   
   def setUp(self) -> None:
      self.log  = LogOutput( self.assertEqual )
      self.log.start()
      
      self.tdir = TempDir( self.assertEqual )
            
      self.tdir.file_make( "FOO", )
      self.tdir.file_make_many( ("FOO","BAR","FUBA",), basepath=["dir1", "dir2", "dir3"] )
      self.tdir.file_make_many( ("database.json",), basepath=(CONFIG.PATH,) )
      
      self.test_dir = self.tdir.test_dir
      os.chdir( self.test_dir )
      self.fs = Filesystem()
      
      
      self.foodir = join( 'dir1','dir2','dir3' )
      self.path_target = CONFIG.PATH
      
   def tearDown( self ):   
      self.log.stop()
      self.tdir.clean()
   
   def test_travel( self ) -> None:
      self.fs.go_up_until( self.path_target, ) 
      items = [x for x in self.fs.recursive_walk_files(".") ]
      self.assertEqual( 4, len(items) )
      sub_foo = join( 'dir1','dir2','dir3','FOO' )
      assert ( sub_foo in items )
      items = [x for x in self.fs.recursive_walk_files("dir1") ]
      self.assertEqual( 3, len(items) )
      assert ( sub_foo in items )

   def test_goup_on_root( self ) -> None:
      self.fs.go_up_until( self.path_target,  ) 
      self.assertEqual( self.test_dir, os.getcwd() )
      conv = self.fs.make_relative("FOO")
      self.assertEqual( 'FOO' , conv )
   
   def goto_test_dir( self ) -> None:
      self.fs = Filesystem( self.test_dir )
      self.fs.go_up_until( self.path_target,  max_levels = 1) 
      os.chdir("/var") # Somthing 
   
   def test_read_nonext( self ) -> None:
      with self.assertRaises( SA_FS_Exception_NotFound ):
         fid = self.fs.file_read( "NO_SUCH_FILE" )
         d = [x for x in fid ]

   def test_read_create( self ) -> None:
      self.goto_test_dir()
      new_fn = join( "NEW_DIR", "NEW_DIR2", "NEW_FILE" )
      meta = Meta( new_fn  )
      meta.modtime = 10**2
      self.fs.file_create( meta, ( bytes("FOO", "utf8"), bytes("BAR", "utf8" ) ) )
      tfile_abs = join( self.test_dir, new_fn )
      self.tdir.file_check( tfile_abs, exists = True )
      content = self.fs.file_read( meta.filename )
      
      # Then try with valid meta
      self.fs.meta_update( meta )
      meta.filename = "NEW_FILE2"
      fid = self.fs.file_read( new_fn )
      self.fs.file_create( meta, fid )
      
   
      
      
   def test_no_cwd( self ) -> None:
      self.goto_test_dir()
      curr_dir =  os.getcwd()
      self.assertEqual( curr_dir, os.getcwd() )
   
   def test_no_cwd_rm( self ) -> None:   
      self.goto_test_dir()
      tfile_rel = join( self.foodir, "FUBA" )
      tfile_abs = join( self.test_dir, tfile_rel )
      
      self.tdir.file_check( tfile_abs, exists = True )
      self.fs.trash_add(  tfile_rel )
      self.tdir.file_check( tfile_abs, exists = False)
      self.fs.trash_revert( tfile_rel )
      self.tdir.file_check( tfile_abs, exists = True )
      
   def test_trash_add_missing( self ):
      self.fs.trash_add( "NONEXISTING", missing_ok = True )
      with self.assertRaises( SA_FS_Exception_NotFound ):
         self.fs.trash_add( "NONEXISTING2", missing_ok = False )
      
   def test_goup_on_sub( self ) -> None:
      os.chdir( self.foodir )
      self.fs = Filesystem()
      self.fs.go_up_until( self.path_target,  ) 
      
      conv = self.fs.make_relative(".")
      sub_foo = self.foodir
      self.assertEqual(  sub_foo, conv )
      
      conv = self.fs.make_relative("FOO")
      sub_foo = join( 'dir1','dir2','dir3','FOO' )
      self.assertEqual(  sub_foo, conv )
      
      # Check that the absolute is proper
      conv = self.fs.make_relative( join( self.test_dir, "FOO") )
      self.assertEqual( 'FOO' , conv )
      
      # Check relative down is ok
      conv = self.fs.make_relative( join( '..','..','..',"FOO" ) )
      self.assertEqual( 'FOO' , conv )
      
      
      
