
import argparse
import os
from typing import List, Set

from sarch.commands import get_commands, CommandFlags
from sarch.database import DatabaseBase, Meta, open_database
from sarch.common import *
from sarch.exceptions import SA_Exception

from sarch.filesystem import Filesystem, SA_FS_Exception_NotFound


def get_parameters_common():
   parser = argparse.ArgumentParser(description='Simple ARCHiving solution.')
   parser.add_argument("--verbose", help="Be more verbose", dest='verbose', action='store_true' )
   parser.add_argument("command", help="What to do")
   return parser


def get_parameters_command( commandline_args, params_wanted_raw ):
     parser = get_parameters_common()
     params_wanted     = []
     params_properties = {}
     
     shorts_taken = set() # type: Set[str]
     
     for key in params_wanted_raw:
        
        params_kwargs = {}
        params_properties[key] = {}
        
        for (k,v) in params_wanted_raw[key].items():
           if not k.startswith("_"):
              params_kwargs[k]=v
           else:
              params_properties[key][k] = v
        
        # Make some short arguments available
        if key.startswith("--") and key[2] not in shorts_taken:
           key_param = (key, key[1:3],)
           shorts_taken.add( key[2] )
        else:
           key_param = (key, )
           
        parser.add_argument( *key_param, **params_kwargs )
        
        key = key.replace("--","")
        params_wanted.append(key)
     params = vars( parser.parse_args( commandline_args ) )
     return ( { key : params[key] for key in params_wanted }, params_properties )


def command_print_help( database, filesystem, help_on ):
    """ Print this help and if command is given as argument print that commands help """
    registered_commands = get_commands()
   
    if help_on == None:
       print_info("Commands:")
       for command in sorted(registered_commands.keys()):
          if command[0] == "_":
             continue
          print_info("%s -- %s" % ( command.ljust(10), registered_commands[command][0].__doc__ ))
       return 0
    
    if help_on not in registered_commands:
      print_info("Command '%s' not registered" % help_on  )
      return -1
 
    (fun,params,opts) = registered_commands[ help_on ]
    print_info("Command '%s':" % help_on )
    print_info( registered_commands[help_on][0].__doc__ )
    print_info("Parameters:")
    
    for key in params:
        try:
           help_text = params[key]["help"]
        except KeyError:
           help_text = ""
        print_info( "  %s : %s " % ( key.ljust(10), help_text ))
    return 0
      

def main( commandline_args : List[str] ) -> int:   
   cmdline, unknown = get_parameters_common().parse_known_args( commandline_args )
   registered_commands = get_commands()
   registered_commands["help"] = ( command_print_help, {'help_on' : {'nargs': '?', 'help': 'Help from what command'} }, 
                                  { CommandFlags.COMMAND_NO_DB : True , CommandFlags.COMMAND_WITH_DIRTY_SYNC : True } )
   
   if cmdline.command not in registered_commands:
     print_error("Command '%s' is not registered." % cmdline.command )
     return -1
   
   if cmdline.verbose == True:
      CONFIG.VERBOSE = 1
      
   fun, params_raw, fun_props = registered_commands[ cmdline.command ]
   params, params_properties = get_parameters_command( commandline_args, params_raw )

   filesystem = Filesystem()
   params["database"] = None
   params["filesystem"] = filesystem
   
   if CommandFlags.COMMAND_NO_DB not in fun_props:
      try:
         filesystem.go_up_until( CONFIG.PATH )
      except SA_FS_Exception_NotFound as err:
         
         if CommandFlags.COMMAND_NO_DB_OK in fun_props:
            database = None
         else:
            print_error("Cannot find repository, root reached.")
            return -1
      else:
         for key, key_prop in params_properties.items():
            if CommandFlags.ARG_IS_PATH in key_prop:
               try:
                  params[key] = filesystem.make_relative( params[key] ) 
               except SA_FS_Exception_NotFound as err:
                  if CommandFlags.ARG_PATH_MAYBE in key_prop:
                     params[key] = filesystem.make_relative( params[key], no_resolve = True ) 
                  else:   
                     print_error( str(err) )
                     return -1
            if CommandFlags.ARG_IS_NOT_RELATIVE_PATH in key_prop:
               try:
                  params[key] = filesystem.make_relative( params[key] ) 
               except:
                  pass # This is the wanted case, it should not be relative.
               else:
                  print_error("Path '%s' is relative to Sarch root. This command requires other path.")
                  return -1
               
                  
         database = open_database( filesystem.make_absolute(CONFIG.PATH) )
         
         if (database.get_status() == DatabaseBase.STATUS_SYNC) and (CommandFlags.COMMAND_WITH_DIRTY_SYNC not in fun_props):
            print_error("Repository is in sync mode. Use 'sync --clear' to reset this or run sync again")
            return -1
            
      
      params["database"] = database
      
   
   
   # The database was not wanted -> we do not open database or change directory
   try:
      ret = fun( **params )
   except SA_Exception as error:
      print_error("%s" % (str(error)))
      ret = -1

   return ret

import sys
if __name__ == "__main__":
   main( sys.argv[1:] )