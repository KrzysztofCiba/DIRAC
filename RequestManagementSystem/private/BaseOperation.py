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

import os
from DIRAC import gLogger, gMonitor, S_ERROR, S_OK
from DIRAC.RequestManagementSystem.Client.Operation import Operation
from DIRAC.FrameworkSystem.Client.ProxyManagerClient import gProxyManager
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getGroupsWithVOMSAttribute

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
  # # private ResourceStatusClient
  __rssClient = None

  def __init__( self, operation = None ):
    """c'tor

    :param Operation operation: Operation instance
    """
    # # placeholders for operation and request
    self.operation = None
    self.request = None
    name = self.__class__.__name__
    self.log = gLogger.getSubLogger( name, True )
    if operation:
      self.setOperation( operation )
    # # std monitor
    for key, val in { "Att": "Attempted ", "Fail" : "Failed ", "Succ" : "Successful " }.items():
      gMonitor.registerActivity( name + key, val + name , name, "Operations/min", gMonitor.OP_SUM )

  def setOperation( self, operation ):
      """ operation and request setter

      :param Operation operation: operation instance
      :raises: TypeError is :operation: in not an instance of Operation
      """
      if not isinstance( operation, Operation ):
        raise TypeError( "expecting Operation instance" )
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

  @classmethod
  def rssClient( cls ):
    """ ResourceStatusClient getter """
    if not cls.__rssClient:
      from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient
      cls.__rssClient = ResourceStatusClient()
    return cls.__rssClient

  def withProxyForLFN( self, lfn ):
    """ get proxy for lfn """
    dirMeta = self.replicaManager().getCatalogDirectoryMetadata( lfn, singleFile = True )
    if not dirMeta["OK"]:
      return dirMeta
    dirMeta = dirMeta["Value"]

    ownerRole = "/%s" % dirMeta["OwnerRole"] if not dirMeta["OwnerRole"].startswith( "/" ) else dirMeta["OwnerRole"]
    ownerDN = dirMeta["OwnerDN"]

    ownerProxy = None
    for ownerGroup in getGroupsWithVOMSAttribute( ownerRole ):
      vomsProxy = gProxyManager.downloadVOMSProxy( ownerDN, ownerGroup, limited = True,
                                                   requiredVOMSAttribute = ownerRole )
      if not vomsProxy["OK"]:
        self.log.debug( "getProxyForLFN: failed to get VOMS proxy for %s role=%s: %s" % ( ownerDN,
                                                                                          ownerRole,
                                                                                          vomsProxy["Message"] ) )
        continue
      ownerProxy = vomsProxy["Value"]
      self.log.debug( "getProxyForLFN: got proxy for %s@%s [%s]" % ( ownerDN, ownerGroup, ownerRole ) )
      break

    if not ownerProxy:
      return S_ERROR( "Unable to get owner proxy" )

    dumpToFile = ownerProxy.dumpAllToFile()
    if not dumpToFile["OK"]:
      self.log.error( "getProxyForLFN: error dumping proxy to file: %s" % dumpToFile["Message"] )
      return dumpToFile
    dumpToFile = dumpToFile["Value"]
    os.environ["X509_USER_PROXY"] = dumpToFile

  def withProxy( self, ownerDN, ownerGroup ):
    """ proxy wrapper """
    ownerProxy = gProxyManager.downloadVOMSProxy( str( ownerDN ), str( ownerGroup ) )
    if not ownerProxy["OK"] or not ownerProxy["Value"]:
      reason = ownerProxy["Message"] if "Message" in ownerProxy else "No valid proxy found in ProxyManager."
      return S_ERROR( "Error when getting proxy for '%s'@'%s': %s" % ( ownerDN, ownerGroup, reason ) )
    ownerProxyFile = ownerProxy["Value"].dumpAllToFile()
    if not ownerProxyFile["OK"]:
      return S_ERROR( ownerProxyFile["Message"] )
    ownerProxyFile = ownerProxyFile["Value"]
    os.environ["X509_USER_PROXY"] = ownerProxyFile
    return S_OK( ownerProxyFile )

  def __call__( self ):
    """ this one should be implemented in the inherited classes

    should return S_OK/S_ERROR
    """
    raise NotImplementedError( "Implement me please!" )
