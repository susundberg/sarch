import json
import os
from pathlib import Path

from typing import Iterable, Set


from .database import *


class DatabaseJson( DatabaseBase ):
   
   DEFAULT_DATABASE = { 'version_major' : 0, 'version_minor' : 1, 'stor' : {}, 'stag' : {}, 'commit' : {}, "status" : DatabaseBase.STATUS_CLEAR }
   
   def __init__(self):
       self.db = {} # type: Dict[ str, Any ]
       self.db_file = None # type: str
       self._find_table_name = None
       self._find_table = None
       
   @staticmethod
   def get_database_file( path : str ) -> str:
      return os.path.join( path, "database.json" )
   
   def get_status( self ) -> DatabaseStatus:
      return self.db["status"]
   
   def set_status( self, status : DatabaseStatus ) -> None:
      self.db["status"] = status
      
   def open_from_path( self, path ) -> None:
      self.db_file = self.get_database_file(path)
      with open( self.db_file ) as fid:
         self.json_loads( fid.read() )
   
   def create_to_path( self, path : str, name : str ) -> None:
      self.db_file = self.get_database_file(path)
      self.db = dict( self.DEFAULT_DATABASE )
      self.db["name"] = name
      self.save()

   def json_dumps( self ) -> str:
      return json.dumps( self.db )
      
   def json_loads( self, json_str ) -> None:
       self.db = json.loads( json_str )
   
   def save( self ) -> None:
      real_target = Path( self.db_file )
      tmp_target  = Path( str(real_target) + ".tmp"  )
      
      # Write first to temporary file, so that we dont get our
      # db corrupted if writing gets cancelled
      with open( str(tmp_target), 'wb') as fid:
         fid.write( bytes( self.json_dumps(), "utf8" )  )
      # Finally rename the file -- this should be almost atomic   
      tmp_target.rename(real_target)
         
   
   def meta_get( self, filename : str ) -> Meta:
       try:
          meta = Meta(filename)
          meta.json_from( self.db["stor"][filename] )
          return meta
       except KeyError:
          raise SA_DB_Exception_NotFound( "File not found from database: '%s'" % filename )

   def meta_find( self, checksum : str ) -> Meta:
       if self._find_table == None or self._find_table_name != "meta_checksum" :
          self._find_table = {}
          idx_find = Meta.JSON_MAPPING.index("checksum")
          for (filename,meta_list) in self.db["stor"].items() :
             cs = meta_list[idx_find]
             if cs != Meta.CHECKSUM_REMOVED and cs != Meta.CHECKSUM_REVERTED:
                self._find_table[cs] = filename 
       
       try:
          filename = self._find_table[checksum]
          return self.meta_get( filename )
       except KeyError:       
          raise SA_DB_Exception_NotFound( "Checksum '%s' not found " % checksum )

   
   def get_table_sizes( self ) -> Tuple[ int, int, int ]:
      return ( len( self.db["commit"]), len( self.db["stor"]), len( self.db["stag"]) )
         
   def meta_set( self, meta : Meta ) -> None:
       self.db["stor"][meta.filename] = meta.json_to()

   def meta_list( self, key_starts_with : str = None ) ->  Iterable[ Meta ]:
      
      key_starts_with = self._prepare_search_key(key_starts_with)
         
      for key, obj in self.db["stor"].items():
         if key_starts_with != None:
            if not key.startswith( key_starts_with ):
               continue
         meta = Meta( key )
         meta.json_from( obj )
         yield meta
         
   def meta_list_keys( self ) -> Iterable[ str ]:
       return self.db["stor"].keys()
   
   def staging_add( self, operation : Operation ) -> None:
       if operation.filename in self.db["stag"]:
          raise SA_DB_Exception("Staging overwrite on '%s' " % operation.filename )
       
       self.db["stag"][operation.filename] = operation.json_to()

   def staging_clear( self ) -> None:
       self.db["stag"] = {}
       
   def staging_get( self, filename : str ) -> Operation:
       try:
          return self.db["stag"][filename]
       except KeyError:
          raise SA_DB_Exception_NotFound( filename )
      
   def staging_list( self ) -> Iterable[ Operation ]:
       for key in sorted( self.db["stag"].keys() ):
          op = Operation( key )
          op.json_from( self.db["stag"][key] )
          yield op
          
   def commit_add( self, commit : Commit ) -> None:
       self.db["commit"][ commit.uid ] = commit.json_to()
       
   def commit_get( self, uid : str ) -> Commit:
      try:
        item = self.db["commit"][ uid ] 
        commit = Commit()
        commit.json_from( item )
        return commit
      except KeyError:
         raise SA_DB_Exception_NotFound( uid )
      
   def commit_list_keys( self ) -> Iterable[ str ]:
      return self.db["commit"].keys()
   
   def commit_list( self, sort_by : str = None, limit : int = 0, keys : Set[str] = None ) -> Iterable[ Commit ]:
      
      source_items = self.db["commit"]
      if keys != None:
         source_items = { k: self.db["commit"][k] for k in keys }
      
      if sort_by == None:
         values = source_items.values()
      else:
         sort_index = Commit.JSON_MAPPING.index( sort_by )
         values = sorted(source_items.values() , key = lambda x: x[sort_index] )
      
      n_returns = 0   
      for item in values:
         n_returns += 1
         commit = Commit()
         commit.json_from( item )
         yield commit 
         if n_returns >= limit:
            return
    







   
   
   
   
   

