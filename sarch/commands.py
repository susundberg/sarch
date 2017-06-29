
import os
import time
import datetime
from pathlib import Path

from typing import Dict, Set, Iterable, TypeVar, Any, Union, Tuple, Callable, Sequence, cast, IO
from collections import OrderedDict

from .filesystem import Filesystem, PathType, SA_FS_Exception_Exists, SA_FS_Exception_NotFound
from .database import DatabaseBase, DatabaseStatus, SA_DB_Exception_NotFound, Operation, Commit, Meta, open_database
from .database_json import DatabaseJson
from .exceptions import SA_Exception
from .common import *
from .remote import remote_sync, remote_open, Remote
from .remote_localfs import RemoteLocalFS

class SA_Cmd_Exception(SA_Exception):
   pass


class CommandFlags:
   ARG_IS_PATH = "_path" # The argument is path and it must be resolved
   ARG_PATH_MAYBE = "_path_non_exiting" # Its ok if resolving above fails.
   ARG_IS_NOT_RELATIVE_PATH = "_path_non_relative"
   COMMAND_NO_DB    = "no_db" # The command must be executed without DB loaded
   COMMAND_NO_DB_OK = "no_db_ok" # The command tries to load db, but its ok if the db is not found
   COMMAND_WITH_DIRTY_SYNC = "db_dirty_sync_ok" # Command can be executed while sync is ongoing
   


CmdProps  = Dict[str, bool ]
CmdParams = Dict[str, Dict[ str, Any ] ] 
CmdFun    = Callable[ ..., int ]
CmdFull   = Dict[ str, Tuple[ Any, CmdParams, CmdProps  ]]

  
_REG_COMMANDS = {} # type: CmdFull

def get_commands() -> CmdFull:
   return _REG_COMMANDS


def _register_command( func : CmdFun , karguments:  CmdParams, props : CmdProps  ):
  assert( func.__name__  not in _REG_COMMANDS)
  _REG_COMMANDS[ func.__name__ ] = ( func, karguments, props)
  


def _staging_exists( db : DatabaseBase , filename : str ):
   try:
      db.staging_get( filename )
      return True
   except SA_DB_Exception_NotFound:
      pass
   
   try:
     meta = db.meta_get( filename )
     if meta.checksum == Meta.CHECKSUM_REVERTED:
        return True
   except SA_DB_Exception_NotFound:
      pass
   return False
   
   

def add( database : DatabaseBase, filesystem : Filesystem, filenames: Iterable[str] ) -> int:
   """ Add file or directory to database """
   had_trouble = 0 
   for abstract_filename in filenames:
      for real_filename in filesystem.recursive_walk_files( abstract_filename ):
         if _staging_exists( database,  real_filename ):
            had_trouble = 1
            print_error("Adding '%s' failed: Operation already pending " % (real_filename) )
            continue

         database.staging_add( Operation(real_filename, Operation.OP_ADD ) )
   database.save()
   return had_trouble

_register_command( add , { "filenames" : {"nargs" : "+", "help" : "Filenames to be added to database", CommandFlags.ARG_IS_PATH : True } },
                         { } ) 

def add_from( database : DatabaseBase, filesystem : Filesystem, filename: str ) -> int:
   """ Add files from given external folder and sort them on current folder based on their modification timestamp """
   target_dir = filesystem.make_relative(".")
   
   fs_other = Filesystem( filename )
   for real_filename in fs_other.recursive_walk_files( "." ):
      modtime = fs_other.get_modtime( real_filename )
      time_prefix = datetime.datetime.fromtimestamp( modtime ).strftime( CONFIG.ADD_FROM_DATE_FORMAT )
      target_file = str( filesystem.make_relative( "%s" % ( Path(time_prefix) / Path(real_filename).name ), no_resolve=True ) )
      target_file_noclash = target_file 
      
      meta_old = Meta( real_filename )
      fs_other.meta_update( meta_old )
      
      loop = 0
      if filesystem.file_exists( target_file_noclash ):
         meta_new  = Meta(target_file_noclash )
         filesystem.meta_update( meta_new )
         if meta_old.check_fs_equal( meta_new ):
            print_info("Files '%s' and '%s' identical, skipping" % (meta_old.filename, meta_new.filename ) )
            fs_other.file_del( real_filename )
            continue
         # Same file exists and different checksum
         while filesystem.file_exists( target_file_noclash ):
           target_file_noclash = "%s-%03d" % ( target_file, loop )
           loop += 1

      # Now, move the file to proper place
      data_stream = fs_other.file_read( real_filename )
      meta_new = meta_old.copy()
      meta_new.filename = target_file_noclash
      filesystem.file_create( meta_new, data_stream )
      database.staging_add( Operation(meta_new.filename, Operation.OP_ADD ) )
      database.save()
      print_info("%s -> %s" % ( meta_old.filename, meta_new.filename ))
   return 0

   
_register_command( add_from , { "filename" : {"help" : "Path to be imported to database", CommandFlags.ARG_IS_NOT_RELATIVE_PATH : True } },
                         { } ) 
   
def rm( database : DatabaseBase, filesystem : Filesystem, filenames: Iterable[str] ) -> int:
   """ Remove file or directory from database and FS """
   
   paths_affected = set() # type: Set[str]
   had_trouble = 0
   
   for abstract_filename in filenames:
      for meta in database.recursive_walk_files( abstract_filename ):
         real_filename = meta.filename
         
         if _staging_exists( database,  real_filename ):
            print_error("Removing '%s' failed: Operation already pending " % (real_filename) )
            had_trouble = 1
            continue 
         
         database.staging_add( Operation(real_filename, Operation.OP_DEL ) )
         filesystem.trash_add( real_filename, missing_ok = True ) 
         paths_affected.add(  filesystem.get_basename( real_filename ) )
         
   filesystem.remove_empty_dirs( paths_affected ) 
   database.save()
   return had_trouble

_register_command( rm,   { "filenames" : {"nargs" : "+", "help" : "Filenames to be removed", CommandFlags.ARG_IS_PATH : True, CommandFlags.ARG_PATH_MAYBE : True } },
                         { } ) 

   
def init( database: DatabaseBase, filesystem : Filesystem, name : str ) -> int:
   """ Initialize new database on this path """   
   
   if database != None or os.path.isdir( CONFIG.PATH ) == True:
      raise SA_Cmd_Exception("The repository exists")
   
   database = DatabaseJson()
   os.makedirs( CONFIG.PATH  )
   database.create_to_path( CONFIG.PATH  , name )
   return 0
   
_register_command( init, { "name" : {"help" : "Name for this database" } },
                         { CommandFlags.COMMAND_NO_DB_OK : True } ) 


def revert( database: DatabaseBase, filesystem : Filesystem, filenames: Iterable[str] ) -> int:
   """ Revert modifications on database, where possible """
   
   def revert_if_modified( database: DatabaseBase, filesystem : Filesystem, fn : str ) -> bool:
      try:
         meta_db = database.meta_get( fn )
      except SA_DB_Exception_NotFound:
         return False
      
      # Reverted 
      if meta_db.checksum == Meta.CHECKSUM_REVERTED:
         print_info("File %s was marked for revert -> clearing the marking" % meta_db.filename )
         return True
      
      meta_fs = Meta( meta_db.filename )
      try:
         filesystem.meta_update( meta_fs )
      except SA_FS_Exception_NotFound:
         pass
      else:   
         if meta_db.check_fs_equal( meta_fs, verbose=False ):
            print_info("File %s fs/db equal, skip" % meta_db.filename )
            return False
      print_info("File %s marked to be reverted -> sync required" % meta_db.filename )
      return True
            
   to_revert = [] # type: List[str]
   filenames_set  = set() # type: Set[str]
   filenames_done = set() # type: Set[str]
   
   # Build list of filenames to be reverted
   for abstract_filename in filenames:   
      for meta in database.recursive_walk_files( abstract_filename ):
         filenames_set.add( meta.filename )

   # Revert all modification on staging list
   for op in database.staging_list():
      if len(filenames_set) == 0 or op.filename in filenames_set:
         if op.operation == Operation.OP_ADD:
            if revert_if_modified( database, filesystem, op.filename ) == True:
                 to_revert.append( op.filename )
         elif op.operation == Operation.OP_DEL:
            try:
               filesystem.trash_revert( op.filename )
            except SA_FS_Exception_NotFound:
               print_info("File %s manually removed, marked to be reverted -> sync required" % op.filename  )
               to_revert.append( op.filename )
         else:
            assert(0) # "Unsupported operation")
         filenames_done.add( op.filename )
   
   # User wants to revert these files but there is no staging operation on them.
   # Check if the file is all good (-> do nothing) but if modified or missing then
   # mark for revert
   for fn in (filenames_set - filenames_done):
      if revert_if_modified( database, filesystem, fn ):
         to_revert.append( fn )
      
   # Clear current staging list      
   database.staging_clear()
   
   # And mark to reverts that require sync
   for fn in to_revert:
      meta = database.meta_get( fn )
      if meta.checksum == Meta.CHECKSUM_REVERTED:
         meta.checksum = Meta.CHECKSUM_NONE
      else:
         meta.checksum = Meta.CHECKSUM_REVERTED
      database.meta_set( meta )
   
   database.save()
   return 0
   
   
_register_command( revert, { "filenames" : {"nargs" : "*", "help" : "Filenames to be removed", CommandFlags.ARG_IS_PATH : True, CommandFlags.ARG_PATH_MAYBE : True } } , { } ) 
   
   

def status( database: DatabaseBase, filesystem : Filesystem ) -> int:
   """ Fast check for untracked or modified file (based on modification timestamp) """
   
   files_fs_no_db  = [] # type: List[str]
   files_fs_mod    = [] # type: List[str]
   files_no_fs_db  = [] # type: List[str]
   files_db_revert = [] # type: List[str]
   
   n_files     = 0 
   checked_files  = {}
   
   
   def print_mod_info( title, files, tag ):
      if len(files) > 0:
         print_info(title)
         for fn in files:
            print_info("#%s: %s" % (tag, fn) )
      return len(files)
   
   staging_dict = {} # type: Dict[ str, Set[str] ]
   for stag in database.staging_list( ):
      if stag.operation not in staging_dict:
         staging_dict[ stag.operation ] = set()
      staging_dict[ stag.operation ].add( stag.filename )
      
   for op in sorted( staging_dict.keys() ):
      print_mod_info( "Pending '%s' operations:" % op, staging_dict[op], op.upper() )
   
   relative_current_path = str(filesystem.make_relative("."))
   for real_filename in filesystem.recursive_walk_files( relative_current_path ):
      n_files += 1
      checked_files[real_filename] = 1
      try:
         meta = database.meta_get( real_filename )
         # There is a file on the disk, that should be have been removed
         if meta.checksum == Meta.CHECKSUM_REMOVED:
            # Check if its on "added" list
            if _staging_exists( database, real_filename ):
               continue
            else:
               files_fs_no_db.append( real_filename )
               continue
         elif meta.checksum == Meta.CHECKSUM_REVERTED:
            continue # Reverted files are not checked
         
         fs_modtime = filesystem.get_modtime( real_filename )
         if meta.modtime != fs_modtime:
            files_fs_mod.append( real_filename )
         
      except SA_DB_Exception_NotFound:
         if _staging_exists( database, real_filename ):
            continue
         files_fs_no_db.append( real_filename )
   

      
   # Then check for files that are not on FS but are on DB
   for meta in database.meta_list( key_starts_with = relative_current_path ):
     if meta.checksum == Meta.CHECKSUM_REVERTED:
        files_db_revert.append( meta.filename )
        
     if _staging_exists( database, meta.filename ):
        continue
     if meta.filename in checked_files:
        continue
     if meta.checksum_normal() == False:
        continue
     
     files_no_fs_db.append( meta.filename )

   
   # We are done! Now just analyze the results
   n_errors = 0
   n_errors += print_mod_info("Untracked files:", files_fs_no_db, "UNT")
   n_errors += print_mod_info("Modified files:", files_fs_mod, "MOD")
   n_errors += print_mod_info("Deleted files:", files_no_fs_db, "DEL")
   n_errors += print_mod_info("To be reverted files:", files_db_revert, "REV")
   if n_errors == 0:
      print_info("%d Files - all good." % n_files)
      return 0
   return 1
_register_command( status, {}, { CommandFlags.COMMAND_WITH_DIRTY_SYNC : True } )
                         

def find_dups( database: DatabaseBase, filesystem : Filesystem ) -> int:
   """ Find checksum duplicates from the database  """
   relative_current_path = str(filesystem.make_relative("."))
   
   checksum_db   = {} # type: Dict[str,str]
   checksum_dups = {} # type: Dict[str,List[str]]
   
   # Bit weird going, but i dont want to store shitload of 1 sized list - as they are 99% of the stuff,
   # so lets rather store the plain filename and make a list on need
   for meta in database.meta_list( key_starts_with = relative_current_path ):
      if not meta.checksum_normal():
         continue
      if meta.checksum in checksum_db:
         try:
            checksum_dups[ meta.checksum ].append( meta.filename ) # type: ignore
         except KeyError:
            checksum_dups[ meta.checksum ] = [ checksum_db[ meta.checksum ] , meta.filename ]
      else:
         checksum_db[ meta.checksum ] = meta.filename
         
   # Full list gone through
   if len(checksum_dups) == 0:
       print_info("No duplicate checksums found.")
       return 0
    
   print_info("Possible (cs matches) duplicate files:")
   unsorted_duplicats = []
   for dupcs in checksum_dups:
       # We could make additional check if the filesize matches
       escaped_names=[ '"%s"' % x for x in sorted(checksum_dups[dupcs]) ]
       unsorted_duplicats.append( " ".join( escaped_names ) )
   
   for item in sorted(unsorted_duplicats):
       print_info(item)
   return 0

_register_command( find_dups, {}, {} )         

   
def log( database: DatabaseBase, filesystem : Filesystem,  filenames: Sequence[str] , count : int ) -> int:
   """ Show registered database events on given files """
   def print_commit_info( commit_list, filename_list ):
      
      if len(filename_list) == 0:
         all_files_ok = True
      else:
         all_files_ok = False
         
      for commit in commit_list:
         full_str = " Commit %s at %s " % ( commit.uid, Commit.time_string( commit.timestamp ) )
         if commit.message:
            full_str += " : %s " % commit.message
         full_str  += "-------------"
            
         print_info( full_str )
         for af in sorted(commit.affected, key=lambda x: x[1] ):
            if all_files_ok or af[1] in filename_list:
               print_info("   %s - %s" % (af[1], af[0]))
      
   commits_affected = set() # type: Set[str]
   files_listed = set()
   
   for abstract_filename in filenames:
      try:
         for meta in database.recursive_walk_files( abstract_filename, only_existing = False):
            for com in meta.last_commits:
               commits_affected.add( com  )
            files_listed.add( abstract_filename )
         
      except SA_DB_Exception_NotFound:
         continue
            
   if len(filenames) == 0:
      commits_affected = None
   
   commit_list = database.commit_list( sort_by = "timestamp", limit=count, keys=commits_affected )
   print_commit_info ( commit_list, files_listed  )
   return 0

_register_command( log, {"filenames" : {"nargs" : "*", "help" : "Check only specific files" },
                         "--count" : {"type" : int, "help" : "How many entries to show", "default" : 16 } } ,
                   { CommandFlags.COMMAND_WITH_DIRTY_SYNC : True } ) 
                    

   
def verify( database: DatabaseBase, filesystem : Filesystem,  filenames: Sequence[str] ) -> int:
   """ Check that given or all files on filesystem correspond to what we have in database - calculate checksum and check modification time. """
   errors = 0
   n_files = 0
   
   def verify_single( meta_db : Meta ) -> int :
      
      if meta_db.checksum_normal() == False:
         return 0

      meta_fs = Meta( meta_db.filename )
      
      try:
         filesystem.meta_update( meta_fs )
      except SA_FS_Exception_NotFound:
         print_error("File '%s' missing" % meta_fs.filename )
         return 1
      
      if meta_fs.check_fs_equal( meta_db ) == False:
         return 1            
      
      print_debug("File '%s' verified ok (md5:%s)." % (meta_db.filename, meta_db.checksum) )
      return 0
   
   metas_list = [] # type: List[Iterable[Meta]]
   if len(filenames) == 0:
      metas_list.append( database.meta_list() )
   else:
      for abstract_filename in filenames:
         metas_list.append( database.recursive_walk_files( abstract_filename , only_existing = True ) )

   # filenames defined, check those
   for metas_list_single in metas_list:
      for meta in metas_list_single:
        n_files += 1
        errors  += verify_single( meta )
     
   if errors == 0:
      print_info("Ok: %d files verified ok." % n_files)
      return 0
   else:
      print_info("Check done: %d errors detected." % errors )
      return 1

_register_command( verify, { "filenames" : {"nargs" : "*", "help" : "Check only specific files" } },
                         { } ) 
              

def _fast_check_for_mods( database: DatabaseBase, filesystem : Filesystem ):
   errors  = 0
   for meta in database.meta_list():
      if meta.checksum_normal() == False and meta.checksum != Meta.CHECKSUM_NONE: 
         continue
      try:
         fs_modtime = filesystem.get_modtime( meta.filename )
      except SA_FS_Exception_NotFound:
         print_error("File '%s' is deleted " % meta.filename )
         continue
         
      if fs_modtime != meta.modtime:
         print_error("File '%s' has modifications" % meta.filename )
         errors += 1
         
   return errors

   
def sync( database: DatabaseBase, filesystem : Filesystem,  url: str ) -> int:
   """ Syncronize this database with given database """
   if "://" not in url:
      url = "file://" + url
   
   # First check that our local database is clean
   local = RemoteLocalFS( "Local" )
   local.open_local( database, filesystem )
   
   if _fast_check_for_mods( database, filesystem ) > 0 :
      print_error("File(s) modified. Commit changes first.")
      return -1
   
   # On local disk we do Additional check that the files are not modified
   
   other = remote_open( url, "Other" )
   
   print_info("Checking and pushing updates .. ")
   remote_sync( local, other )
   
   if local.xtable.done() and other.xtable.done():
      print_info("Everything up to date.. ")
      return 0
   
   def database_store( remote : Remote, status : DatabaseStatus ):
      # First, we mark the databe that its going to be under sync
      remote.db.set_status( status )
      # And save the database
      remote.database_save()
      
   
   # Ok we have something to do, first, save the current xtables
   database_store( local, DatabaseBase.STATUS_SYNC )
   database_store( other, DatabaseBase.STATUS_SYNC )
      
   print_info("Transferring & syncing files .. ")
   local.execute_sync( other )
   other.execute_sync( local )
   
   # Then save the database changes, and clear the xtable
   database_store( local, DatabaseBase.STATUS_CLEAR )
   database_store( other, DatabaseBase.STATUS_CLEAR )
   other.close()
   
   print_info("Sync completed! ")
   return 0
_register_command( sync, {"url" : {"help" : "Url to other repository" } } , { CommandFlags.COMMAND_WITH_DIRTY_SYNC : True } ) 
                    


def _commit_scan_for_auto( database: DatabaseBase, filesystem : Filesystem ):
   
   for meta in database.meta_list():
      if meta.checksum == Meta.CHECKSUM_REMOVED or meta.checksum == Meta.CHECKSUM_REVERTED:
         continue
      
      try:
        fs_modtime = filesystem.get_modtime( meta.filename )
      except SA_FS_Exception_NotFound:
        fs_modtime = None
      
      if fs_modtime == None:
        database.staging_add( Operation(meta.filename, Operation.OP_DEL ) )
      else:
        if fs_modtime != meta.modtime or meta.checksum == Meta.CHECKSUM_NONE:
          database.staging_add( Operation(meta.filename, Operation.OP_ADD ) )
   
              
def commit( database: DatabaseBase, filesystem : Filesystem, msg : str = None, auto : bool = False ) -> int:
   """ Commit changes (add, del, move) to database and filesystem """
   
   
   # Then seek for all modified files
   pending_ops = database.staging_list()
      
   # Generate new commit UID
   commit = Commit( msg )
   pending_adds = []
   
   if auto == True:
      _commit_scan_for_auto( database, filesystem )
      
   for op in pending_ops :
     if op.operation == Operation.OP_ADD:
        try:
           meta = database.meta_get( op.filename )
           meta_cs_orig = meta.checksum
           
           fs_modtime = filesystem.get_modtime( op.filename)
           if meta.modtime == fs_modtime and meta_cs_orig != Meta.CHECKSUM_REMOVED:
              # No update, nothing to do for this file
              continue
           
           filesystem.meta_update( meta ) 
           if meta_cs_orig != Meta.CHECKSUM_REMOVED:
              op.operation = Operation.OP_MODIFY
           # Else this is removed file that is re-added, marke as add
           
        # File is not found from database, make new      
        except SA_DB_Exception_NotFound:
           meta = Meta( op.filename )
           pending_adds.append( op.filename )
           filesystem.meta_update( meta )
           
        print_info("Added %s with checksum %s" % ( meta.filename, meta.checksum ))   
        meta.add_commit( commit )
        database.meta_set( meta )
        
     elif op.operation == Operation.OP_DEL:
        meta = database.meta_get( op.filename )
        meta.checksum = Meta.CHECKSUM_REMOVED # Mark file deleted
        meta.modtime  = filesystem.make_time( time.time() )
        meta.add_commit( commit )
        print_info("Deleted %s " % ( meta.filename ))
        database.meta_set( meta )

     else:
        assert(0) # Unsupported commit operation 
        
     # Anyway, mark this commit to contain following operation
     commit.operation_append( op )
     
   # All operation processed ok. Make make them read only
   for fn in pending_adds:
      filesystem.file_make_readonly( fn )  

   database.staging_clear()
   
   # And remove all files that were marked for deletion   
   filesystem.trash_clear( )

   if commit.operation_count() == 0:
      print_info("No operations to be done.")
   else:
      print_info("%d changes commited ok." % commit.operation_count() )
      database.commit_add( commit )
      
   database.save()
   return 0

_register_command( commit, { "--msg" : {"help" : "Additional message for this commit", "default" : "" },
                             "--auto" : {"help" : "Automatically add modified files and deleted files", "action" : "store_true" }},
                         { } ) 


def _server_mode( database: DatabaseBase, filesystem : Filesystem, path:str ) -> int:
   """ Runs the server mode for remote connections """
   from .remote_ssh import remote_ssh_server
   import sys
   
   
   # Drop the none arguments 
   filesystem = Filesystem( path )
   database = open_database( filesystem.make_absolute(CONFIG.PATH) )
   
   set_output_to( sys.stderr )
   sys.stdout.flush()
   sys.stdin.flush()
   
   if _fast_check_for_mods( database, filesystem ) > 0 :
      print_error("Remote has local modifications. Please commit changes there and try again.")
      return -1
   
   pipe_in  = open( sys.stdin.fileno(), 'rb', closefd=False )
   pipe_out = open( sys.stdout.fileno(), 'wb', closefd=False )
   remote_ssh_server( database, filesystem, pipe_in, pipe_out ) 
   return 0
   
_register_command( _server_mode, {"path" : { "help" : "The basepath for the repository"}}, {CommandFlags.COMMAND_NO_DB : True } )
                                  
   
   
