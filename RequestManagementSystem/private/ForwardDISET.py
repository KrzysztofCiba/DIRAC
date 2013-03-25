########################################################################
# $HeadURL $
# File: ForwardDISET.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2013/03/22 12:40:06
########################################################################

""" :mod: ForwardDISET 
    =======================
 
    .. module: ForwardDISET
    :synopsis: DISET forwarding operation handler
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    DISET forwarding operation handler
"""

__RCSID__ = "$Id $"

##
# @file ForwardDISET.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/03/22 12:40:22
# @brief Definition of ForwardDISET class.

## imports 
from DIRAC import S_OK, S_ERROR, gMonitor
from DIRAC.RequestManagementSystem.private.BaseOperation import BaseOperation
from DIRAC.Core.DISET.RPCClient import executeRPCStub
from DIRAC.Core.Utilities import DEncode


########################################################################
class ForwardDISET(BaseOperation):
  """
  .. class:: ForwardDISET
  
  """
  def __init__( self, operation ):
    """ c'tor 
    
    :param Operation operation: an Operation instance
    """
    ## call base class ctor
    BaseOperation.__init__( self, operation )
    

 
  def __call__( self ):
    """ execute RPC stub """
    ## update monitor for attempted
    gMonitor.addMark( "Attempted", 1 )
    ## get arguments
    rpcStubString = self.operation.Arguments
    rpcStub, length = DEncode.decode( rpcStubString )
    forward = executeRPCStub( rpcStub )
    if not forward["OK"]:
      gMonitor.addMark( "Failed", 1 )
      self.log.error("unable to execute '%s' operation: %s" % ( self.operation.Type, forward["Message"] ) )
      return forward
    self.log.info("forwarding done")
    gMonitor.addMark( "Successful", 1 )
    self.operation.Status = "Done"
    return S_OK()
    
    
