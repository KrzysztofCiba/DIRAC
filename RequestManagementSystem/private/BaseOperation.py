########################################################################
# $HeadURL $
# File: BaseOperation.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2013/03/13 13:48:52
########################################################################
""" :mod: BaseOperation 
    =======================
 
    .. module: BaseOperation
    :synopsis: request operation handler base class
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    request operation handler base class
    
    this should be a functor getting Operation as ctor argument
    __call__ should return S_OK/S_ERROR

"""
__RCSID__ = "$Id $"
##
# @file BaseOperation.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/03/13 13:49:02
# @brief Definition of BaseOperation class.

from DIRAC import gLogger, gMonitor


########################################################################
class BaseOperation(object):
  """
  .. class:: BaseOperation

  request operation handler base class
  """
  ## private replica manager
  __replicaManager = None
  ## private data logging client
  __dataLoggingClient = None
  ## private monitor
  __monitor = None

  def __init__( self, operation ):
    """c'tor

    :param self: self reference
    :param Operation operation: Operation instance
    """
    ## save operation
    self.operation = operation
    ## keep request protected
    self._request = operation._parent
    ## std monitor
    gMonitor.registerActivity( "Attempted", "Processed Operations", 
                               self.__class__.__name__, "Requests/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "Successful", "Successful Operations", 
                                self.__class__.__name__, "Requests/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "Failed", "Failed Operations", 
                                self.__class__.__name__, "Requests/min", gMonitor.OP_SUM )

    ## own logger
    self.log = gLogger.getSubLogger( "%s/%s/%s" % ( self._request.RequestName,
                                                    self._request.Order,
                                                    self.operation.Type ) )
  @classmethod
  def replicaManager( cls ):
    """ ReplicaManger getter """
    if not cls.__replicaManager:
      from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
      cls.__replicaManager = ReplicaManager()
    return cls.__replicaManager

  @classmethod
  def dataLoggingClient( cls ):
    """ DataLoggingClient getter """
    if not cls.__dataLoggingClient:
      from DIRAC.DataManagementSystem.Client.DataLoggingClient import DataLoggingClient
      cls.__dataLoggingClient = DataLoggingClient()
    return cls.__dataLoggingClient

  @classmethod
  def monitor( cls ):
    """ gMonitor facade """
    if not cls.__monitor:
      from DIRAC import gMonitor
      cls.__monitor = gMonitor
    return cls.__monitor

  def __call__( self ):
    """ call me maybe 
    
    this one should return S_OK/S_ERROR
    """
    raise NotImplementedError("Implement me please!")
