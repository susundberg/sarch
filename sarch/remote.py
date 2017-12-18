
from typing import Iterable, Tuple, Union, List, Dict, Set, Sequence, Callable
from abc import abstractmethod, ABCMeta

from .exceptions import SA_Exception
from .common import CONFIG, print_debug, print_info, read_input
from .database import DatabaseBase, Meta, Commit, SA_DB_Exception_NotFound
from .filesystem import Filesystem, SA_FS_Exception_NotFound

class SA_SYNC_Exception( SA_Exception ):
   pass

class SA_SYNC_Exception_Cancelled( SA_Exception ):
   pass


class SyncTable:
   def __init__( self, name  ):
      self.copy       = [] # type: List[Meta] 
      self.delete     = [] # type: List[Meta] 
      self.merged     = [] # type: List[Meta] 
      self.move       = [] # type: List[Tuple[Meta,Meta]]
      self.copy_local = [] # type: List[Tuple[Meta,Meta]]  
      self.name = name

   def done( self ):
      for to_check in [self.copy, self.delete, self.merged, self.move, self.copy_local ]:
         if len(to_check) > 0:
            return False
      return True
   
   def detect_move_files( self, db : DatabaseBase ):
      rmfrom_copy = []
      rmfrom_delete = []
      
      to_delete_files =  { meta.filename : index for index, meta in enumerate(self.delete) }
      
      for (index, meta_copy) in enumerate(self.copy):
         try:
            meta_old = db.meta_find( checksum = meta_copy.checksum )
            rmfrom_copy.append( index )
            assert( meta_old.checksum == meta_copy.checksum )
            
            if meta_old.filename in to_delete_files:
               index_in_delete = to_delete_files[meta_old.filename]
               meta_old = self.delete[ index_in_delete ]
               self.move.append( (meta_old, meta_copy) )
               op = "move"
               rmfrom_delete.append( index_in_delete )
            else:
               self.copy_local.append( (meta_old, meta_copy) )
               op = "copy_local"
            print_debug("#SYNC:%s: %s from %s in %s" % (meta_copy.filename, op, meta_old.filename, self.name ) )
         except SA_DB_Exception_NotFound:
            continue
      
      def filter_list( what, indices ):
        return [ meta for (index,meta) in enumerate(what) if index not in indices ]
     
      self.copy = filter_list( self.copy, rmfrom_copy )
      self.delete = filter_list( self.delete , rmfrom_delete )
      #print("DELETE LIST:" + str(rmfrom_delete))
      
         
   def merge( self, meta : Meta ):
       self.merged.append( meta )
       
   def process( self, meta : Meta ):
       if meta.checksum == Meta.CHECKSUM_REMOVED:
          self.delete.append( meta )
          print_debug("#SYNC:%s: delete %s " % (meta.filename, self.name) )
       else:
          assert( meta.checksum != Meta.CHECKSUM_REVERTED )
          self.copy.append( meta )
          print_debug("#SYNC:%s: copy mod %s " % (meta.filename, self.name) )
          
   def append_missing_files( self, filenames : Set[str], db : DatabaseBase ):
      for fn in filenames:
         meta = db.meta_get( fn )
         if meta.checksum_normal() == False : # This file is missing and marked for revert or deleted
            print_debug("#SYNC:%s: merge missing %s" % (meta.filename, self.name) )
            self.merged.append( meta ) # Do add the meta anyway
         else:
            self.copy.append( meta )
            print_debug("#SYNC:%s: copy new %s " % (meta.filename, self.name) )


class Filestatus:
   FILE_OVERWRITE_OK = "partial"
   FILE_EQUAL = "ok"
   pass
   


def check_file_equal( meta: Meta, database : DatabaseBase, filesystem : Filesystem ) -> str:
   
      try:
         meta_fs = meta.copy()
         filesystem.meta_update( meta_fs )
      except SA_FS_Exception_NotFound:
         return Filestatus.FILE_OVERWRITE_OK # File missing
         
      if meta_fs.check_fs_equal( meta, verbose=False ):
         return Filestatus.FILE_EQUAL # File already as wanted
      
      # Now this might be also partial file from the transfer. That we detect by checking the FS trashbin
      if filesystem.trash_exists( meta.filename ):
          return Filestatus.FILE_OVERWRITE_OK
      
      try:
         meta_db = database.meta_get( meta.filename )
         
         if meta_db.checksum == Meta.CHECKSUM_REVERTED:
            return Filestatus.FILE_OVERWRITE_OK
         
         # When overwriting normal old file, we have file cs matching to DB cs.
         if meta_db.check_fs_equal( meta_fs , verbose=False):
            return Filestatus.FILE_OVERWRITE_OK

      # The file is not on database, its untracked, file -> troubles   
      except SA_DB_Exception_NotFound:
         pass
      
         
      
      return ("File '%s' exists as untracked file. It would be overwritten. Bailing out." % meta.filename )

def check_database( database : DatabaseBase ):
      for item in database.staging_list():
        raise SA_SYNC_Exception_Cancelled("Database has staging operations. Commit changes and try again" )

class Remote( metaclass=ABCMeta ):

   def __init__(self, name : str ) -> None:
       self.db = None # type: DatabaseBase
       self.xtable = None # type: SyncTable
       self.name = name 
      
   @abstractmethod
   def database_get( self ) -> DatabaseBase:
      pass
   
   @abstractmethod
   def database_save( self ) -> None:
      pass
   
   @abstractmethod
   def close( self ) -> None:
      pass

   @abstractmethod
   def open( self, url : str ) -> None:
     pass
  
   @abstractmethod
   def file_get( self, source : Meta ) -> Iterable [bytes]:
      pass
   
   @abstractmethod
   def file_set( self, target : Meta, content: Iterable [bytes] ) -> None:
      pass
   
   @abstractmethod
   def file_del( self, target : Meta ) -> None:
      pass

   @abstractmethod
   def file_move( self, source : Meta, target : Meta ) -> None:
      pass
   
   @abstractmethod
   def file_copy( self, source : Meta, target : Meta ) -> None:
      pass
   
   def _xtable_set( self, xtable : SyncTable ) -> None:
      self.xtable = xtable
      
   def execute_sync( self,  other : 'Remote' ) -> None:
      for item in sorted(self.xtable.copy, key=lambda meta: meta.filename ):
         print_debug("Repo %s: Transfer %s"  %( self.name, item.filename ) )
         fid = other.file_get( item  )
         self.file_set( item, fid )
         self.db.meta_set( item )
      
      for item_source, item_target in sorted(self.xtable.copy_local,  key=lambda tup: tup[0].filename ):
         print_debug("Repo %s: Copy local %s -> %s" %( self.name, item_source.filename, item_target.filename ) )
         self.file_copy( item_source, item_target )
         self.db.meta_set( item_target )
                  
      for item_source, item_target in sorted(self.xtable.move, key=lambda tup: tup[0].filename ):
         print_debug("Repo %s: Move %s -> %s "  % ( self.name, item_source.filename, item_target.filename ) )
         self.file_move( item_source, item_target )
         self.db.meta_set( item_source )
         self.db.meta_set( item_target )

      for item in sorted(self.xtable.delete, key=lambda meta: meta.filename ):
         print_debug("Repo %s: Delete %s"  %( self.name, item.filename ))
         self.file_del( item )
         self.db.meta_set( item )

      for meta in self.xtable.merged:
         self.db.meta_set( meta )


def remote_open( url : str, name : str ) -> Remote:
   remote = None # type: Remote
   if url.startswith("file://"):
      from .remote_localfs import RemoteLocalFS
      remote = RemoteLocalFS( name )
   elif url.startswith("ssh://"):
      from .remote_ssh import RemoteSSH
      remote = RemoteSSH( name )
   else:
      raise SA_SYNC_Exception("Unknown protocol '%s'" % url )
   remote.open( url )
   return remote

   
def remote_sync( local:Remote, other: Remote ) -> None :
   
   db_local = local.database_get()
   db_other = other.database_get()
   
   xtable_local = SyncTable("Local")
   xtable_other = SyncTable("Other")
   
   # Check meta files
   local_metas = set( db_local.meta_list_keys() )
   other_metas = set( db_other.meta_list_keys() )
   
   # First check files that are only in other db
   xtable_other.append_missing_files( local_metas - other_metas, db_local )
   # another way around
   xtable_local.append_missing_files( other_metas - local_metas, db_other )

   # Then check rest of the files.
   conflicts = _build_process_common_files( xtable_local, xtable_other, db_local, db_other, other_metas & local_metas, )
   
   _solve_conflicts( xtable_local, xtable_other, db_local, db_other, conflicts )
   
   # Then try to find moved files to avoid transfer between repositories
   xtable_local.detect_move_files( db_local )
   xtable_other.detect_move_files( db_other )
      
   # Sync all commit objects
   local_commits = set( db_local.commit_list_keys() )
   other_commits = set( db_other.commit_list_keys() )
   _append_commits( other_commits - local_commits, db_local, db_other ) 
   _append_commits( local_commits - other_commits, db_other, db_local ) 
    
   # And we are done
   local._xtable_set( xtable_local )
   other._xtable_set( xtable_other ) 
   




def _solve_conflicts( xtable_local : SyncTable, xtable_other : SyncTable, db_local : DatabaseBase, db_other : DatabaseBase,
                      conflicts : Sequence[ Tuple[ Meta, Meta ] ] ) -> None:
   
   loop = 0
   
   def _print_file_log( name, meta, db ):
      print_info(" ---- %s info below ----- " % name )
      print_info(" Checksum: %s " % meta.checksum )
      print_info(" Last modified: %s " % Meta.time_string( meta.modtime ) )
      print_info(" Last 5 commits: ")
      
      def _print_commit( commit ):
         print_info("    Commit: %s at %s - msg: %s" % ( commit.uid, Commit.time_string( commit.timestamp ), commit.message ))
      
      for commit_index in range( min(5,len(meta.last_commits))):
         commit = db.commit_get( meta.last_commits[ -1 - commit_index ] )
         _print_commit( commit )
         
   for meta_local, meta_other in conflicts:
      loop += 1
      print_info("Conflict (%d/%d) on file '%s'. Manual resolve required." % ( loop, len(conflicts), meta_local.filename ) )
      _print_file_log( xtable_local.name , meta_local, db_local  )
      _print_file_log( xtable_other.name , meta_other, db_other )
      print_info("------------------------------------")
      
      resp = read_input( ("l","o","x"), "Select (L)ocal or (O)thers or e(X)it and cancel sync." )
      
      if resp == 'l':
         xtable_other.process( meta_local )
      elif resp == 'o':
         xtable_local.process( meta_other )
      else:
         print_info("Cancelling synnc and bailing out.")
         raise SA_SYNC_Exception_Cancelled()
   

def _find_common_commit( commits_local : Sequence[str], commits_other : Sequence[str] ) -> Tuple[int,int]:
   
   # As most of the files should have same last commit, check that one first
   if commits_local[-1] == commits_other[-1]:
      return (0,0)
   
   if len( commits_local ) < len( commits_other ):
      shorter = commits_local 
      longer  = commits_other
      short_first = True
   else:
      shorter = commits_other
      longer  = commits_local 
      short_first = False
   
   shorter_reversed = { uid : index for index,uid in enumerate( reversed( shorter ) )  }
   for index_longer,item in enumerate( reversed(longer) ):
      try:
         index_shorter  = shorter_reversed[ item ]
      except KeyError:
         continue
      
      if short_first:
         return (index_shorter, index_longer)
      return ( index_longer, index_shorter )
   
   # No common commit found
   return (-1,-1)      
      
   
          
def _build_process_common_files( xtable_local : SyncTable, xtable_other : SyncTable, 
                          db_local : DatabaseBase, db_other : DatabaseBase,
                          filenames : Set[str] ) ->   Sequence[ Tuple[ Meta, Meta ] ]  :
    
    conflicts = [] # type: List[ Tuple[ Meta, Meta ] ]
    nfiles_ok = 0
    for fn in sorted(filenames):

       # Check what is this file status -- do we have something in common
       meta_local = db_local.meta_get( fn )
       meta_other = db_other.meta_get( fn )
       
       
       (idx_local, idx_other ) = _find_common_commit( meta_local.last_commits, meta_other.last_commits )
       
       # We common commit, and in addition to that its the last commit in both 
       if idx_local == 0 and idx_other == 0 :
          # Check if this is reverted file
          local_revert = (meta_local.checksum == Meta.CHECKSUM_REVERTED)
          other_revert = (meta_other.checksum == Meta.CHECKSUM_REVERTED)
          
          if local_revert == True and other_revert == True:
             print_debug("#SYNC:%s: File marked for revert in both db -> skip " % meta_local.filename )
          elif local_revert == True:
             xtable_local.copy.append( meta_other )
          elif other_revert == True:
             xtable_other.copy.append( meta_local )
          else:
             # Do additional check
             if meta_local.checksum == meta_other.checksum:
                nfiles_ok += 1
                pass
             else:
                raise SA_SYNC_Exception("File '%s' in both db and as last commit, but checksum differs. DB corruption." % meta_local.filename )
       
       # No common commits or both files have modifications 
       elif idx_local < 0 or ( idx_local > 0 and idx_other > 0): 

          # They seem to have same checksum, lets treat them like the same file. Lets take local
          if meta_local.check_fs_equal( meta_other, verbose = False ):
             xtable_local.merge( meta_local )
             xtable_other.merge( meta_local )
             print_debug("#SYNC:%s: merge identical" % meta_local.filename )
          else:   
             # Lets ask user to tell us which file he likes more.
             conflicts.append( (meta_local, meta_other) )
             print_info(" Conflict: %s" % meta_local.filename )
             
       # The local index is zero, and other > 0 -> other has modifications and those needs to be 
       # merged to local. 
       elif idx_local == 0 :
          xtable_local.process( meta_other )
       # Same as above
       elif idx_other == 0 :
          xtable_other.process( meta_local )
       else: # Oh its extra line, we never get here
          assert(0)
    
    print_debug("#SYNC: %d files skipped, as identical in both db  " % nfiles_ok )   
    return conflicts       
       




def _append_commits( commits : Set[str], db_to : DatabaseBase, db_from : DatabaseBase ):
   
   for commit in commits:
      db_to.commit_add( db_from.commit_get( commit ) )
      


