
import os
from typing import Sequence, TextIO
import sys

class CONFIG:
   PATH=".sarch"
   VERBOSE=0
   PATH_TRASH=os.path.join( ".sarch", "trash" )
   PATH_SEPARATOR = "/"
   DATA_BLOCK_SIZE = (2**20)
   SSH_COMMAND = "ssh"
   VERSION = "1.0.0"
   
output = print
output_error = print

def set_output_to( io : TextIO ):
   global output 
   global output_error
   
   def factory_output_to_pipe( io, prefix="" ):
     def printer( what : str, file = None):
        io.write( prefix + what + "\n" )
     return printer
   output = factory_output_to_pipe( io )
   output_error = factory_output_to_pipe( io, prefix = "Error: " )

   
def print_error( what : str ):
   output_error("Error: " + what, file=sys.stderr )
   
def print_info( what : str):
   output( what )
   
def print_debug( what : str ):
   if CONFIG.VERBOSE:
      output( what )

def read_input( options : Sequence[str] , explain : str ):
   output( explain )
   
   while True:
      response = input(explain).lower()
      if response not in options:
         continue
      return response
      
   
   
   