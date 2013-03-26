########################################################################
# $HeadURL $
# File: BaseOperation.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2013/03/13 13:48:52
########################################################################
""" :mod: BaseOperation
    ===================

    .. module: BaseOperation
    :synopsis: request operation handler base class
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    request operation handler base class

    this should be a functor getting Operation as ctor argument
    __call__ should return S_OK/S_ERROR

"""
__RCSID__ = "$Id $"
# #
# @file BaseOperation.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/03/13 13:49:02
# @brief Definition of BaseOperation class.

from DIRAC import gLogger, gMonitor

########################################################################
class BaseOperation( object ):
  """
  .. class:: BaseOperation

  request operation handler base class
  """
  # # private replica manager
  __replicaManager = None
  # # private data logging client
  __dataLoggingClient = None

  def __init__( self, operation = None ):
    """c'tor

    :param Operation operation: Operation instance
    """
    if operation:
      self.setOperation( operation )
    # # std monitor
    name = self.__class__.__name__
    for key, val in { "Att": "Attempted ", "Fail" : "Failed ", "Succ" : "Successful " }.items():
      gMonitor.registerActivity( name + key, val + name , name, "Operations/min", gMonitor.OP_SUM )

  def setOperation( self, operation ):
      """ operation setter """
      self.operation = operation
      self.request = operation._parent
      self.log = gLogger.getSubLogger( "%s/%s/%s" % ( self.request.RequestName,
                                                      self.request.Order,
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

  def __call__( self ):
    """ this one should be implemented in the inherited classes

    should return S_OK/S_ERROR
    """
    raise NotImplementedError( "Implement me please!" )
