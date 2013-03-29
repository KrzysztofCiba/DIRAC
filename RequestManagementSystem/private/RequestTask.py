########################################################################
# $HeadURL $
# File: RequestTask.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2013/03/13 12:42:45
########################################################################

""" :mod: RequestTask
    =================

    .. module: RequestTask
    :synopsis: request processing task
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    request processing task to be used inside ProcessTask created in RequestAgent
"""

__RCSID__ = "$Id $"

# #
# @file RequestTask.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/03/13 12:42:54
# @brief Definition of RequestTask class.

# # imports
import os
from DIRAC import gLogger, S_OK, S_ERROR, gMonitor
from DIRAC.RequestManagementSystem.Client.RequestClient import RequestClient
from DIRAC.RequestManagementSystem.Client.Request import Request
from DIRAC.RequestManagementSystem.private.BaseOperation import BaseOperation
from DIRAC.FrameworkSystem.Client.ProxyManagerClient import gProxyManager

########################################################################
class RequestTask( object ):
  """
  .. class:: RequestTask

  request's processing task
  """
  # # request client
  __requestClient = None

  def __init__( self, requestXML, handlersDict ):
    """c'tor

    :param self: self reference
    :param str requestXML: request serilised to XML
    :param dict opHandlers: operation handlers
    """
    self.request = Request.fromXML( requestXML )["Value"]
    # # handlers dict
    self.handlersDict = handlersDict
    # # handlers class def
    self.handlers = {}
    # # own sublogger
    self.log = gLogger.getSubLogger( self.request.RequestName )
    # # own gMonitor activities
    gMonitor.registerActivity( "RequestAtt", "Requests processed",
                               "RequestTask", "Requests/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "RequestFail", "Requests failed",
                               "RequestTask", "Requests/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "RequestOK", "Requests done",
                               "RequestTask", "Requests/min", gMonitor.OP_SUM )

  def setupProxy( self ):
    """ download and dump request owner proxy to file and env

    :return: S_OK with name of newly created owner proxy file
    """
    ownerProxy = gProxyManager.downloadVOMSProxy( str( self.request.OwnerDN ), str( self.request.OwnerGroup ) )
    if not ownerProxy["OK"] or not ownerProxy["Value"]:
      reason = ownerProxy["Message"] if "Message" in ownerProxy else "No valid proxy found in ProxyManager."
      return S_ERROR( "Change proxy error for '%s'@'%s': %s" % ( self.request.OwnerDN,
                                                                 self.request.OwnerGroup,
                                                                 reason ) )
    ownerProxyFile = ownerProxy["Value"].dumpAllToFile()
    if not ownerProxyFile["OK"]:
      return S_ERROR( ownerProxyFile["Message"] )
    ownerProxyFile = ownerProxyFile["Value"]
    os.environ["X509_USER_PROXY"] = ownerProxyFile
    return S_OK( ownerProxyFile )

  @staticmethod
  def loadHandler( pluginPath ):
    """ Create an instance of requested plugin class, loading and importing it when needed.
    This function could raise ImportError when plugin cannot be find or TypeError when
    loaded class object isn't inherited from BaseOperation class.
    :param str pluginName: dotted path to plugin, specified as in import statement, i.e.
    "DIRAC.CheesShopSystem.private.Cheddar" or alternatively in 'normal' path format
    "DIRAC/CheesShopSystem/private/Cheddar"

    :return: object instance
    This function try to load and instantiate an object from given path. It is assumed that:

    - :pluginPath: is pointing to module directory "importable" by python interpreter, i.e.: it's
    package's top level directory is in $PYTHONPATH env variable,
    - the module should consist a class definition following module name,
    - the class itself is inherited from DIRAC.RequestManagementSystem.private.BaseOperation.BaseOperation
    If above conditions aren't meet, function is throwing exceptions:

    - ImportError when class cannot be imported
    - TypeError when class isn't inherited from BaseOepration
    """
    if "/" in pluginPath:
      pluginPath = ".".join( [ chunk for chunk in pluginPath.split( "/" ) if chunk ] )
    pluginName = pluginPath.split( "." )[-1]
    if pluginName not in globals():
      mod = __import__( pluginPath, globals(), fromlist = [ pluginName ] )
      pluginClassObj = getattr( mod, pluginName )
    else:
      pluginClassObj = globals()[pluginName]
    if not issubclass( pluginClassObj, BaseOperation ):
      raise TypeError( "operation handler '%s' isn't inherited from BaseOperation class" % pluginName )
    for key, status in ( ( "Att", "Attempted" ), ( "OK", "Succeeded" ) , ( "Fail", "Failed" ) ):
      gMonitor.registerActivity( "%s%s" % ( pluginName, key ), "%s %s" % ( pluginName, status ),
                                 pluginName, "Operations/min", gMonitor.OP_SUM )
    # # return an instance
    return pluginClassObj

  def getHandler( self, operation ):
    """ return instance of a handler for a given operation type on demand
        all created handlers are kept in self.handlers dict for further use

    :param Operation operation: Operation instance
    """
    if operation.Type not in self.handlersDict:
      return S_ERROR( "handler for operation '%s' not set" % operation.Type )
    handler = self.handlers.get( operation.Type, None )
    if not handler:
      try:
        handlerCls = self.loadHandler( self.handlersDict[operation.Type] )
        self.handlers[operation.Type] = handlerCls()
        handler = self.handlers[ operation.Type ]
      except ( ImportError, TypeError ), error:
        self.log.exception( "getHandler: %s" % str( error ), lException = error )
        return S_ERROR( str( error ) )
    # # set operation for this handler
    handler.setOperation( operation )
    # # and return
    return S_OK( handler )

  @classmethod
  def requestClient( cls ):
    """ on demand request client """
    if not cls.__requestClient:
      cls.__requestClient = RequestClient()
    return cls.__requestClient

  def updateRequest( self ):
    """ put back request to the RequestDB """
    updateRequest = self.requestClient().updateRequest( self.request )
    if not updateRequest["OK"]:
      self.log.error( updateRequest["Message"] )
    return updateRequest

  def __call__( self ):
    """ request processing """

    gMonitor.addMark( "RequestAtt", 1 )

    setupProxy = self.setupProxy()
    if not setupProxy["OK"]:
      self.log.error( setupProxy["Message"] )
      self.request.Error = setupProxy["Message"]
      return self.updateRequest()

    while self.request.Status == "Waiting":

      # # get waiting operation
      operation = self.request.getWaiting()
      if not operation["OK"]:
        self.log.error( operation["Message"] )
        return operation
      operation = operation["Value"]
      gMonitor.addMark( "%s%s" % ( operation.Type, "Att" ), 1 )

      # # and handler for it
      handler = self.getHandler( operation )
      if not handler["OK"]:
        self.log.error( "unable to process operation %s: %s" % ( operation.Type, handler["Message"] ) )
        gMonitor.addMark( "%s%s" % ( operation.Type, "Fail" ), 1 )
        operation.Error = handler["Message"]
        break
      handler = handler["Value"]

      # # and execute
      try:
        exe = handler()
        if not exe["OK"]:
          self.log.error( "unable to process operation %s: %s" % ( operation.Type, exe["Message"] ) )
          gMonitor.addMark( "%s%s" % ( operation.Type, "Fail" ), 1 )
          gMonitor.addMark( "RequestFail", 1 )
          break
      except Exception, error:
        self.log.exception( "hit by exception: %s" % str( error ) )
        gMonitor.addMark( "%s%s" % ( operation.Type, "Fail" ), 1 )
        gMonitor.addMark( "RequestFail", 1 )
        break

      # # operation status check
      if operation.Status == "Done":
        gMonitor.addMark( "%s%s" % ( operation.Type, "OK" ), 1 )
      elif operation.Status == "Failed":
        gMonitor.addMark( "%s%s" % ( operation.Type, "Fail" ), 1 )
      elif operation.Status in ( "Waiting", "Scheduled" ):
        break

    # # request done?
    if self.request.Status == "Done":
      self.log.always( "request done" )
      gMonitor.addMark( "RequestOK", 1 )
      # # and there is a job waiting for it? finalize!
      if self.request.JobID:
        finalizeRequest = self.requestClient.finalize( self.request, self.request.JobID )
        if not finalizeRequest["OK"]:
          self.log.error( "unable to finalize request %s: %s" % ( self.request.RequestName,
                                                                  finalizeRequest["Message"] ) )
          return finalizeRequest

    # # update request to the RequestDB
    return self.updateRequest()
