from abc import abstractmethod, ABCMeta
from typing import TypeVar, List, Tuple, Type, Iterable, Set, Union, NewType
from uuid import uuid1 as make_uid
import datetime
import time
from copy import deepcopy

from .exceptions import SA_Exception
from .common import CONFIG, print_info, print_error


class SA_DB_Exception( SA_Exception ):
   pass

class SA_DB_Exception_NotFound( SA_DB_Exception ):
   pass



DBValue = TypeVar('DBValue', int, str)

class DatabaseObjectBase:
   JSON_MAPPING = ("",) 
   
   def json_from( self, dobj : List[DBValue] ):
     for loop,attr in enumerate(self.JSON_MAPPING):
        setattr( self, attr, dobj[loop])
        
   #def check_all_properties( self ) -> bool:
     #for loop,attr in enumerate(self.JSON_MAPPING):
        #if getattr( self, attr ) == None:
           #return False
     #return True 
  
   @staticmethod
   def time_string( timestamp: Union[int, float] ) -> str:
      return datetime.datetime.fromtimestamp( timestamp ).strftime('%Y-%m-%d %H:%M:%S')   
                   
   def copy( self ):
      return deepcopy( self )
   
   #def attributes_equal( self, other : 'DatabaseObjectBase', verbose : bool = False, skip : List['str'] = [] ) -> bool:
     #for loop,attr in enumerate(self.JSON_MAPPING):
        
        #if attr in skip:
           #continue
        
        #my_attr    = getattr( self, attr )
        #other_attr = getattr( other, attr )
        #if my_attr != other_attr:
           #if verbose:
              #print_info("File '%s' attribute '%s' differs '%s' vs '%s' " % ( 
                         #self.filename, attr, my_attr, other_attr )) 
           #return False
     #return True
      
      
   def json_to( self ) -> List[DBValue]:
     ret = []
     for loop,attr in enumerate(self.JSON_MAPPING):
        ret.append( getattr( self, attr ) )
     return ret   

   
class Meta(DatabaseObjectBase):
   JSON_MAPPING=( 'modtime','checksum','last_commits')
   
   CHECKSUM_REMOVED  = "#FILE_REMOVED"
   CHECKSUM_NONE     = ""
   CHECKSUM_REVERTED = "#FILE_REVERT" 
   
   def __init__(self, filename: str ) -> None:
      self.filename = filename
      self.modtime  = 0
      self.checksum = self.CHECKSUM_NONE
      self.last_commits = [] # type: List[ str ]
      assert( filename[0] != '/' )
   
   def add_commit( self, commit : 'Commit' ) -> None:
      self.last_commits.append( commit.uid )
      
   def checksum_normal( self ) -> bool:
      if self.checksum == self.CHECKSUM_NONE:
         return False
      return (self.checksum[0] != "#")
   
   def check_fs_equal( self, meta_other : 'Meta', verbose : bool = True ) -> bool:
      for attr_name  in ["checksum", "modtime" ]:
         v_fs = getattr( self, attr_name )
         v_db = getattr( meta_other, attr_name )
         if v_fs != v_db:
            if verbose:
               print_error("File '%s' attribute '%s' differs on fs: '%s' vs on db: '%s' " % 
                           ( self.filename, attr_name, v_fs, v_db  ))
            return False
      return True

      
class Commit( DatabaseObjectBase ):      
   JSON_MAPPING=( 'uid', 'timestamp', 'message', 'affected', )
   
   def __init__(self, message : str = "" ) -> None:
      self.uid       = str(make_uid())
      self.timestamp = time.time()
      self.message   = message
      self.affected  = [] # type: List[ Tuple [str, str, str] ]
   
   def operation_append( self, op : 'Operation' ) -> None:
      self.affected.append( ( op.filename, op.operation, op.extra ) ) 
   
   def operation_count( self ) -> int:
      return len ( self.affected )
   
   
   
   
class Operation( DatabaseObjectBase ):
   JSON_MAPPING=( 'operation', 'extra' )
   
   OP_ADD = "add"
   OP_DEL = "del"
   OP_MODIFY = "mod"
   OP_REVERT    = "rev"
   
   OPERATIONS = (OP_ADD, OP_DEL, OP_MODIFY, OP_REVERT, )
   
   def __init__(self, filename : str, operation : str = None, extra : str = None ) -> None:
      
      self.filename  = filename
      self.operation = None
      self.extra     = extra
      
      if operation != None:
         assert( operation in self.OPERATIONS )
         self.operation = operation
   
            
def open_database( path : str ) -> 'DatabaseBase':
   from .database_json import DatabaseJson
   
   db = DatabaseJson()
   db.open_from_path( path )
   return db

   
DatabaseStatus = NewType('DatabaseStatus', str )

class DatabaseBase( metaclass=ABCMeta ):
    
    STATUS_SYNC  = DatabaseStatus("sync")
    STATUS_CLEAR = DatabaseStatus("ok")
    
    def __init__( self ):
      pass
   
    @abstractmethod
    def get_status( self ) -> DatabaseStatus:
       """ Return the current status of the database """
       pass
   
    @abstractmethod
    def set_status( self, status : DatabaseStatus ) -> None:
       """ Set the status """
       pass
   
    @abstractmethod
    def json_dumps( self ) -> str:
       """ Give current database as json """
       pass
      
    @abstractmethod
    def json_loads( self, str ) -> None:
       """ Load given json into database """
       pass
   
    @abstractmethod
    def open_from_path( self, path : str ):
      """ Open the datase file from given path. """
      pass
      
    @abstractmethod  
    def create_to_path( self, path : str, name : str ):
      """ Create new database to this given path """
      pass
    
    @abstractmethod  
    def save( self ):
      """ Commit all changes and close the database """
      pass
   
    @abstractmethod
    def get_table_sizes( self ) -> Tuple[ int, int, int ]:
      """ Return tuple for n-commits, n-stor, n-staging """
      pass
   
    @abstractmethod  
    def meta_find( self, checksum : str ) -> Meta:
       pass
    
    @abstractmethod  
    def meta_get( self, filename : str ) -> Meta:
       """ Return metadata from the filename """
       pass
    
    @abstractmethod  
    def meta_set( self, meta : Meta ):
       pass
    
    @abstractmethod  
    def meta_list( self, key_starts_with : str = None ) ->  Iterable[ Meta ]:
       pass
          
    @abstractmethod  
    def meta_list_keys( self ) -> Iterable[ str ]:
       pass

    @abstractmethod  
    def staging_add( self, operation : Operation ):
       """ Add filename with given operation to db """
       pass
    
    @abstractmethod  
    def staging_list( self ) -> Iterable[ Operation ]:
       pass

    @abstractmethod  
    def staging_clear( self ):
       pass
    
    @abstractmethod  
    def staging_get( self, filename : str ) -> Operation:
       """ Returns if there is operation on staging list for the given filename """
       pass
       
    @abstractmethod  
    def commit_add( self, commit : Commit ) -> None:
       pass
    
    @abstractmethod  
    def commit_get( self, uid : str ) -> Commit:
       pass    
    
    @abstractmethod  
    def commit_list_keys( self ) -> Iterable[ str ]:
       pass
    
    @abstractmethod
    def commit_list( self, sort_by : str = None, limit : int = 0, keys : Set[str] = None ) -> Iterable[ Commit ]:
       pass
    
    def _prepare_search_key( self, key_starts_with ):
       if key_starts_with == None:
          return None
       if len(key_starts_with) > 0 and key_starts_with[0] == ".":
          key_starts_with = key_starts_with[1:]
       if len(key_starts_with) == 0:
          return None
       return key_starts_with 
    
    def recursive_walk_files( self, filename_raw : str, only_existing : bool = True ) -> Iterable[ Meta ]:
       """ Returns iterable list of filenames that match the given input. Some of the files might be removed """
       
       return_files = 0
    
       def check_give_error( filename_raw : str ):
          raise SA_DB_Exception_NotFound("No matching files:" + filename_raw )
       
       try:
          meta = self.meta_get( filename_raw )
          if only_existing == False or meta.checksum != Meta.CHECKSUM_REMOVED:
             yield meta
          else:
             check_give_error( filename_raw )
          return

       except SA_DB_Exception_NotFound :
          pass
       
       # Ok, now direct match on the filename, lets try if it would be a directory
       # the argument might be 'foo' or 'foo/'
       
       if filename_raw[-1] != CONFIG.PATH_SEPARATOR:
          filename_raw = filename_raw + CONFIG.PATH_SEPARATOR
       
       metas = self.meta_list( key_starts_with=filename_raw )
    
       for meta in metas:
          if only_existing == False or meta.checksum != Meta.CHECKSUM_REMOVED:
             yield meta
             return_files += 1
       
       if return_files == 0:
          check_give_error( filename_raw )
          
       
       
       
       
       








