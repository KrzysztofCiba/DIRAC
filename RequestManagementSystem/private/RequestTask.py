########################################################################
# $HeadURL $
# File: RequestTask.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2013/03/13 12:42:45
########################################################################

""" :mod: RequestTask
    =======================

    .. module: RequestTask
    :synopsis: request processing task
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    request processing task
"""

__RCSID__ = "$Id $"

# #
# @file RequestTask.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/03/13 12:42:54
# @brief Definition of RequestTask class.

# # imports
from DIRAC import gLogger, S_OK, gMonitor
from DIRAC.RequestManagementSystem.Client.RequestClient import RequestClient
from DIRAC.RequestManagementSystem.Client.Request import Request

########################################################################
class RequestTask( object ):
  """
  .. class:: RequestTask

  request's processing task
  """
  # # request client
  __requestClient = None

  def __init__( self, requestXML, handlers ):
    """c'tor

    :param self: self reference
    :param str requestXML: request serilised to XML
    :param dict opHandlers: operation handlers
    """
    self.request = Request.fromXML()["Value"]
    self.handelrs = handlers
    self.log = gLogger.getSubLogger( self.request.RequestName )
    gMonitor.registerActivity( "RequestsAtt", "Requests processed",
                               "RequestTask", "Requests/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "RequestsFail", "Requests failed",
                               "RequestTask", "Requests/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "RequestsDone", "Requests done",
                               "RequestTask", "Requests/min", gMonitor.OP_SUM )

  @classmethod
  def requestClient( cls ):
    """ on demand request client """
    if not cls.__requestClient:
      cls.__requestClient = RequestClient()
    return cls.__requestClient

  def makeGlobal( self, objName, objDef ):
    """ export :objDef: to global name space using :objName: name

    :param self: self reference
    :param str objName: symbol name
    :param mixed objDef: symbol definition
    :throws: NameError if symbol of that name is already in
    """
    if objName not in __builtins__:
      if type( __builtins__ ) == type( {} ):
        __builtins__[objName] = objDef
      else:
        setattr( __builtins__, objName, objDef )

  def __call__( self ):
    """ request processing """
    gMonitor.addMark( "RequestsAtt", 1 )
    while self.request.getWaiting():
      operation = self.request.getWaiting()
      if operation.Type not in self.handlers:
        self.log.error( "handler for '%s' operation not defined" % operation.Type )
        break
      try:
        ret = self.handlers[operation.Type]( operation )()
        if not ret["OK"]:
          # # bail out on error
          self.log.error( ret["Message"] )
          return ret
      except Exception, error:
        self.log.exception( error )
        break

    if self.request.Status == "Done":
      gMonitor.addMark( "RequestsDone", 1 )
      if self.request.JobID:
        finalizeRequest = self.requestClient.finalize( self.request, self.request.JobID )
        if not finalizeRequest["OK"]:
          self.log.error( "unable to finalize request %s: %s" % ( self.request.RequestName,
                                                                 finalizeRequest["Message"] ) )
          return finalizeRequest
    else:
      gMonitor.addMark( "RequestsFail", 1 )

    updateRequest = self.requestClient().updateRequest( self.request )
    if not updateRequest["OK"]:
      self.log.error( updateRequest["Message"] )
      return updateRequest
    # # if we're here request has been processed
    return S_OK()


