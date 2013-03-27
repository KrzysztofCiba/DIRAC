########################################################################
# $HeadURL $
# File: ForwardDISET.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2013/03/22 12:40:06
########################################################################
""" :mod: ForwardDISET
    ==================

    .. module: ForwardDISET
    :synopsis: DISET forwarding operation handler
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    DISET forwarding operation handler
"""

__RCSID__ = "$Id $"

# #
# @file ForwardDISET.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/03/22 12:40:22
# @brief Definition of ForwardDISET class.

# # imports
from DIRAC import S_OK, S_ERROR, gMonitor
from DIRAC.RequestManagementSystem.private.BaseOperation import BaseOperation
from DIRAC.Core.DISET.RPCClient import executeRPCStub
from DIRAC.Core.Utilities import DEncode

########################################################################
class ForwardDISET( BaseOperation ):
  """
  .. class:: ForwardDISET

  functor forwarding DISET operations
  """
  def __init__( self, operation = None ):
    """ c'tor

    :param Operation operation: an Operation instance
    """
    # # call base class c'tor
    BaseOperation.__init__( self, operation )

  def __call__( self ):
    """ execute RPC stub """

    # # update monitor for attempted
    gMonitor.addMark( "ForwardDISETAtt", 1 )

    # # decode arguments
    try:
      decode = DEncode.decode( self.operation.Arguments )
    except ValueError, error:
      self.log.exception( error )
      self.operation.Error = str( error )
      self.operation.Status = "Failed"
      gMonitor.addMark( "ForwardDISETFail", 1 )
      return S_ERROR( str( error ) )
    
    forward = executeRPCStub( decode[0] )
    if not forward["OK"]:
      gMonitor.addMark( "ForwardDISETFail", 1 )
      self.log.error( "unable to execute '%s' operation: %s" % ( self.operation.Type, forward["Message"] ) )
      return forward

    self.log.info( "DISET forwarding done" )
    gMonitor.addMark( "ForwardDISETSucc", 1 )
    self.operation.Status = "Done"
    return S_OK()
