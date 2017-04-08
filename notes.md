
# Implementation notes for developers

## Notes from typing:

* Cannot cast same variable into different type: ``url_parts = url[6:].split(":")``
* If return type depends on parameter given, you need casting: 

      def _send( self, *pargs : Union[str,int] , wanted : str = None, data : Iterable [bytes] = None ) -> Union[str, Iterable [bytes] ]:
          ...
          resp = self._send( "get", source.filename, source.checksum, wanted="data")
          return cast( Iterable [bytes], resp )


* There was bug with this at ubuntu latest python, but it is fixed in python GIT

       CmdFun    = Callable[ ..., int ] # Any number of arguments, returns int
       CmdFull   = Tuple[ str, CmdFun ] # Error! It should be legal!

* Unittesting:

        self.remote.ssh  = MagicMock() # type: ignore
       
* It gets bit troublesome if you want to do something like registry of command functions that take some parameters .. 

      CmdProps  = Dict[str, bool ]
      CmdParams = Dict[str, Dict[ str, Any ] ] 
      CmdFun    = Callable[ ..., int ]
      CmdFull   = Dict[ str, Tuple[ CmdFun , CmdParams, CmdProps  ]]

* Returning None is by default ok, even if you except say int.

* Server running debian 8 - its python 3.4 by default -> manual compiling, ughh

## Notes other:

* Buffers have changed in python3 -- sys.stdin.read1()
