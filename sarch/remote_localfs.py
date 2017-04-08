
from typing import Iterable

from .database import DatabaseBase, open_database, Meta
from .filesystem import Filesystem, SA_FS_Exception_NotFound
from .common import CONFIG

from .remote import Remote, SA_SYNC_Exception, SA_SYNC_Exception_Cancelled, check_file_equal, check_database, Filestatus

class RemoteLocalFS( Remote ):

   def file_get( self, source : Meta ) -> Iterable [bytes]:
      return self.fs.file_read( source.filename )
   
   def file_set( self, target : Meta, content: Iterable [bytes] ) -> None:
      status = check_file_equal( target, self.db, self.fs ) 
      if status == Filestatus.FILE_OVERWRITE_OK:
         self.fs.file_create( target, content )
      elif status == Filestatus.FILE_EQUAL:
         return 
      else:
         raise SA_SYNC_Exception_Cancelled( status )
   
   def file_del( self, target : Meta ) -> None:
      self.fs.file_del( target.filename, missing_ok = True )

   def file_move( self, source : Meta, target : Meta ) -> None:
      status = check_file_equal( target, self.db, self.fs ) 
      if status == Filestatus.FILE_OVERWRITE_OK:
         # the whole file is missing
         self.fs.move( source.filename, target.filename, create_dirs=True, modtime = target.modtime )
      elif status == Filestatus.FILE_EQUAL:
         # We need to remove the move-to file if such still exists
         self.fs.file_del( source.filename, missing_ok = True )
      else:
         raise SA_SYNC_Exception_Cancelled( status )
   
   def file_copy( self, source : Meta, target : Meta ) -> None:
      status = check_file_equal( target, self.db, self.fs ) 
      if status == Filestatus.FILE_OVERWRITE_OK:
         fid = self.fs.file_read( source.filename )
         self.fs.file_create( target, fid )
      elif status == Filestatus.FILE_EQUAL:
         return 
      else:
         raise SA_SYNC_Exception_Cancelled( status )
      
   def database_get( self ):
      return self.db
   
   def database_save( self ):
      self.db.save()

   def close( self ) -> None:
      self.fs.trash_clear()
      pass
   
   def _open_check( self ):       
      self.fs.trash_clear()
      check_database( self.db )
      
   def open_local( self,  database : DatabaseBase = None , filesystem : Filesystem = None ):
      self.db = database
      self.fs = filesystem
      self._open_check()
      
      
   def open( self, url : str ):
      assert( url.startswith("file://") )
      # replace the file://
      url = url[7:]
      try:
         self.fs = Filesystem( url )
      except FileNotFoundError:
         raise SA_SYNC_Exception("Repository path not existing: '%s'" % url )
      try:
         self.fs.go_up_until( CONFIG.PATH,  max_levels = 1 )
      except SA_FS_Exception_NotFound:
         raise SA_SYNC_Exception("Repository not found from path: '%s'" % url )
      self.db = open_database( Filesystem.join( url, CONFIG.PATH ) )
      self._open_check()

   
