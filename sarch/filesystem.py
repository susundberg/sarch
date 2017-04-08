
import os
import hashlib
import shutil

from pathlib import Path
from typing import Iterable, Tuple, Union, Dict, Set, Sequence

from .exceptions import SA_Exception
from .database import Meta
from .common import CONFIG, print_debug

class SA_FS_Exception( SA_Exception  ):
   pass

class SA_FS_Exception_NotFound( SA_FS_Exception ):
   pass

class SA_FS_Exception_Exists( SA_FS_Exception ):
   pass

class SA_FS_Exception_UnSupportedType( SA_FS_Exception ):
   pass

class SA_FS_Exception_ChecksumError( SA_FS_Exception ):
   pass

class PathType: 
   FILE   = "FILE"
   DIR    = "DIR"
   OTHER  = "OTHER"
   NONEXT = "NONEXT"
   
   
class Filesystem:
   
   
   
   def __init__(self, path : str = None ) -> None:
      
      if path == None:
         path_obj = Path.cwd().resolve()
      else:
         path_obj = Path( path ).resolve()
      self.path_init = path_obj
      self.path_current = Path( self.path_init )   
   
   @staticmethod
   def join( *pargs ) :
      return str( Path( *pargs ) ) 
   
   #@staticmethod
   #def path_root( filename : str ) -> str:
      #path = Path( filename )
      #if len(path.parts) > 1:
         #return path.parts[0]
      
      
   
   def go_up_until( self, target_dir : str,  max_levels : int = None  ) -> None:
      
      if max_levels == None:
         max_levels = len(self.path_init.parts)
      path_current = Path( self.path_init )
         
      for n_levels_up in range(max_levels):
         path_target  = path_current / target_dir
         if path_target.is_dir() == True:
            self.path_current = path_current 
            break
         path_current = (path_current / "..").resolve()
      else:      
         raise SA_FS_Exception_NotFound("Limit reached, path not found")

   
   def _make_absolute( self, target : Union[ Path, str ] ) -> Path:
      return Path( self.path_current, target )
   
   def make_absolute( self, source : str ):
      return str(self._make_absolute( source))

   def _make_relative_single( self, raw_path : Union[ Path, str ], no_resolve : bool = True ) -> str:
      if no_resolve == False:
         try:
            conv = Path( self.path_init, raw_path ).resolve()
         except FileNotFoundError as err:
            raise SA_FS_Exception_NotFound(str(err))
      else:
         conv = Path( self.path_init, raw_path )
      related = conv.relative_to( self.path_current )
      return str( related )

   def make_relative( self, raw_path : Union[ str, Sequence[str]] , no_resolve : bool = False ) -> Union[ str, Sequence[str]]:
      if type(raw_path) == str:
         return self._make_relative_single( str(raw_path), no_resolve )
      return [ self._make_relative_single( x, no_resolve ) for x in raw_path ]
   
   def is_blacklisted( self, path: str ):
      if path.startswith( CONFIG.PATH ):
         return True
   
   def get_basename( self, path: Union[ Path, str ] ) -> str:
      return str( Path( path ).parent )
   
   @staticmethod
   def make_time( timestamp : float ) -> int:
      return int( timestamp )
                 
   def get_modtime( self, path: Union[ Path, str ] ) -> int:
      target = self._make_absolute( path )
      try:
         disk_time = target.stat().st_mtime 
      except FileNotFoundError:
         raise SA_FS_Exception_NotFound("File not found %s" % path )
      return self.make_time( disk_time )
      
   def recursive_walk_files( self, abstract_filename : str ) -> Iterable[str]:
      target_absolute = self._make_absolute( abstract_filename )
      target_relative = self._make_relative_single( str(target_absolute) )
         
      if self.is_blacklisted( target_relative ):
         return tuple()
      elif target_absolute.is_file():
         yield target_relative
      elif target_absolute.is_dir():
         for item in target_absolute.iterdir():
            yield from self.recursive_walk_files( str(item) )
      elif target_absolute.exists() == False:
         raise SA_FS_Exception_NotFound( "File does not exists: %s" % target_relative )
      else:
         raise SA_FS_Exception_UnSupportedType( target_relative )
   
   
   def _checksum_init( self ):
      return hashlib.md5()
   
   def file_create( self, meta : Meta, data_source : Iterable[bytes] ):
      path = self._make_absolute( meta.filename )
      
      self.make_directories( path.parent )
      
      if meta.checksum != Meta.CHECKSUM_NONE:
         cs_calc = self._checksum_init()
      else:
         cs_calc = None
      
      tlen = 0    
      tmp_file = self._trash_prepare( meta.filename )
      
      with open(  str(tmp_file) , 'wb' ) as fid:
         for data_in in data_source:
            if cs_calc:
                cs_calc.update(data_in)
            fid.write( data_in )
            tlen += len(data_in)
            
      self._file_set_modtime( str(tmp_file), meta.modtime )
      
      if cs_calc != None and cs_calc.hexdigest() != meta.checksum:
            raise SA_FS_Exception_ChecksumError("Checksum on file '%s' differs (calc: %s stored: %s), sized: %d" % (meta.filename, cs_calc.hexdigest(), meta.checksum, tlen ))
      
      tmp_file.rename( path )
      print_debug("Created file %s: %s" % (meta.filename, meta.checksum ))
   
   def file_exists( self, filename : str ):
      path = self._make_absolute( filename )
      return path.exists()
   
   def file_del( self, filename : str, missing_ok = False):
       path = self._make_absolute( filename )
       try:
          path.unlink()
       except FileNotFoundError:
          if missing_ok == True:
             return 
          raise SA_FS_Exception_NotFound("File not found %s" % filename )
       
   def _file_set_modtime( self, filename : Union[ Path, str ], modtime : int ):
      os.utime( str(filename), (modtime, modtime ) )
      
   def file_read( self, filename : str ):
      path = self._make_absolute( filename )
      try:
         with open( str(path), 'rb' ) as fid:
            while True:
               data = fid.read( CONFIG.DATA_BLOCK_SIZE )
               if len(data) == 0:
                  return
               yield data
      except FileNotFoundError:
        raise SA_FS_Exception_NotFound("File not found %s" % filename )
     
   def meta_update( self, meta : Meta ):
      path = self._make_absolute( meta.filename )
      
      cs_calc = self._checksum_init()
      meta.modtime  = self.get_modtime( path )
      data_n = 0
      try:
         with path.open('rb') as fid:
            while True:
               data = fid.read(  CONFIG.DATA_BLOCK_SIZE )
               data_n += len(data)
               if len(data) == 0:
                  break
               cs_calc.update(data)
            meta.checksum = cs_calc.hexdigest()
      except FileNotFoundError:
        raise SA_FS_Exception_NotFound("File not found %s" % meta.filename )   
      return data_n
   
   def remove_empty_dirs( self, to_check : Set[str] ) -> None : 
      for item in sorted( to_check ):
         self._recursive_remove_empty_dirs( self._make_absolute( item ) )
         
      
   def _recursive_remove_empty_dirs( self, path : Path ) -> None:
       original_len = len(path.parts)
       while path.exists() == False:
           path = path.parent
           if len(path.parts) == 1:
              break
       if path.is_dir() == False:
          return 
       for path_loop in range( original_len ):
          n_items = 0 
          for item in path.iterdir():
             return
          # No items in the directory, this can be removed
          path.rmdir()
          path = path.parent 
   
   def move( self, source_file : str, target_file : str, dry_run : bool = False, create_dirs : bool = False, modtime:int = None ) -> str:
      """ Move given file and return the new filename """
      source = self._make_absolute(source_file)
      target = self._make_absolute(target_file)
      
      if target.is_dir():
         target_full = target / source.name
      else:
         target_full = target 
         
      # The full target may not exists
      if target_full.exists():
         raise SA_FS_Exception_Exists("Move target file '%s' exists" % target_full )
      
      if dry_run == False:
         if create_dirs == True:
            target_base = target_full.parent
            self.make_directories( target_base )
         source.rename( target_full )
      
      if modtime != None:
         self._file_set_modtime( target_full, modtime )
         
      return str( self._make_relative_single( target_full ) )
   
      
   def make_directories( self, path : Union[ Path, str ] ) -> None:
      source = self._make_absolute( path )
      source.mkdir( parents=True, exist_ok=True )
      
   def _trash_prepare( self, path : str ) -> Path:
      target_full = self._make_absolute( Path( CONFIG.PATH_TRASH, path ) )
      target_base = Path( self.get_basename( target_full ) )
      self.make_directories( target_base )
      return target_full
   
   def trash_add( self, path : str, missing_ok : bool = False ):
      """ Add given file to trash """
      source = self._make_absolute( path )
      target_full = self._trash_prepare( path )
      try:
         source.rename( target_full )
      except FileNotFoundError:
         if missing_ok == True:
            return 
         else:
            raise SA_FS_Exception_NotFound( "File not found: " + str(source) )
      
      
   def trash_clear( self ) -> None:
      """ Really remove the files from FS """
      trash_path = self._make_absolute( CONFIG.PATH_TRASH )
      if trash_path.exists() == False:
         return
      if trash_path.is_dir():
         shutil.rmtree( str( trash_path ) )
         return
      
      raise SA_FS_Exception("Trash directory '%s' is not directory!" % str( trash_path ) )
   
   def trash_exists( self, filename : str ) -> bool:
      target_full = self._trash_prepare( filename )
      return target_full.exists()
   
   def trash_revert( self, filename : str ) -> None:
      """ Move single file out from the trash """
      source = self._make_absolute( Path( CONFIG.PATH_TRASH, filename ) )
      target = self._make_absolute( filename )
      target_base = Path( self.get_basename( target ) )
      self.make_directories( target_base )
      try: 
         source.rename( target )
      except FileNotFoundError:
         raise SA_FS_Exception_NotFound( "File not found: " + str(source) )
      
   def file_make_readonly( self, fn : str ):
      pass
  
     
     
     
     
      