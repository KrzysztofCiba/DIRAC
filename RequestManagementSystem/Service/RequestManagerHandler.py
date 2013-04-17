#####################################################################
# $HeadURL $
# File: RequestManagerHandler.py
########################################################################
""" :mod: RequestManagerHandler 
    ===========================

    .. module: RequestManagerHandler
    :synopsis: Implementation of the RequestDB service in the DISET framework
"""
__RCSID__ = "$Id$"
## imports 
from types import DictType, IntType, ListType, StringTypes
## from DIRAC
from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.DISET.RequestHandler import RequestHandler
## from RMS
from DIRAC.RequestManagementSystem.Client.Request import Request
from DIRAC.RequestManagementSystem.private.RequestValidator import RequestValidator

## global RequestDB instance
gRequestDB = None

def initializeRequestManagerHandler(serviceInfo):
  """ initialise handler """
  global gRequestDB
  from DIRAC.RequestManagementSystem.DB.RequestDB import RequestDB
  gRequestDB = RequestDB()
  return S_OK()

class RequestManagerHandler(RequestHandler):
  """
  .. class:: RequestManagerHandler
  
  RequestDB interface in the DISET framework.
  """
  ## request validator
  validator = None

  ## helper functions 
  @classmethod
  def validate( cls, request ):
    """ request validation """
    if not cls.validator:
      cls.validator = RequestValidator()
    return cls.validator.validate( request )

  @staticmethod
  def __getRequestID( requestName ):
    """ get requestID for given :requestName: """
    requestID = requestName
    if type(requestName) in StringTypes:
      result = gRequestDB.getRequestProperties( requestName, [ "RequestID" ] )
      if not result["OK"]:
        return result
      requestID = result["Value"]
    return S_OK( requestID )

  types_putRequest = [ StringTypes ]
  @classmethod
  def export_putRequest( cls, requestString ):
    """ put a new request into RequestDB 

    :param cls: class ref
    :param str requestString: xml string
    """
    gLogger.info("RequestManager.putRequest: Setting request %s"  % requestString )
    requestName = "***UNKNOWN***"
    try:
      request = Request.fromXML( requestString )
      if not request["OK"]:
        gLogger.error("RequestManager.putRequest: %s" % request["Message"] )
        return request
      request = request["Value"]
      valid =  cls.validate( request )
      if not valid["OK"]:
        gLogger.error( "RequestManagerHandler.putRequest: request not valid: %s" % valid["Message"] )
        return valid
      requestName = request.RequestName
      gLogger.info("RequestManagerHandler.putRequest: Attempting to set request '%s'" % requestName )   
      return gRequestDB.putRequest( request )
    except Exception, error:
      errStr = "RequestManagerHandler.putRequest: Exception while setting request."
      gLogger.exception( errStr, requestName, lException=error )
      return S_ERROR(errStr)
    
  types_getDBSummary = []
  @staticmethod
  def export_getDBSummary():
    """ Get the summary of requests in the Request DB """
    gLogger.info("RequestManagerHandler.getDBSummary: Attempting to obtain database summary.")
    try:
      return gRequestDB.getDBSummary()
    except Exception, error:
      errStr = "RequestManagerHandler.getDBSummary: Exception while getting database summary."
      gLogger.exception( errStr, lException=error )
      return S_ERROR(errStr)

  types_getRequest = [ StringTypes ]
  @staticmethod
  def export_getRequest( requestName = "" ):
    """ Get a request of given type from the database """
    gLogger.info("RequestHandler.getRequest: Attempting to get request")
    try:
      getRequest = gRequestDB.getRequest( requestName )
      if not getRequest["OK"]:
        gLogger.error( "RequestHandler.getRequest: %s" % getRequest["Message"] )
        return getRequest
      return S_OK( getRequest["Value"].toXML() ) if getRequest["Value"] else getRequest 
    except Exception, error:
      errStr = "RequestManagerHandler.getRequest: Exception while getting request."
      gLogger.exception( errStr, lException=error )
      return S_ERROR(errStr)

  types_getRequestSummaryWeb = [ DictType, ListType, IntType, IntType ]
  @staticmethod
  def export_getRequestSummaryWeb( selectDict, sortList, startItem, maxItems):
    """ Get summary of the request/subrequest info in the standard form for the web

    :param dict selectDict: selection dict
    :param list sortList: ???
    :param int startItem: start item
    :param int maxItems: max items
    """
    gLogger.info("RequestManagerHandler.getRequestSummeryWeb called")
    try:
      return gRequestDB.getRequestSummaryWeb( selectDict, sortList, startItem, maxItems )
    except Exception, error:
      errStr = "RequestManagerHandler.getRequestSummaryWeb: Exception while getting request."
      gLogger.exception( errStr, lException=error )
      return S_ERROR(errStr)
   
  types_deleteRequest = [ StringTypes ]
  @staticmethod
  def export_deleteRequest( requestName ):
    """ Delete the request with the supplied name"""
    gLogger.info( "deleteRequest: Deleting request '%s'..." % requestName )
    try:
      return gRequestDB.deleteRequest( requestName )
    except Exception, error:
      errStr = "deleteRequest: Exception which deleting request '%s'." % requestName
      gLogger.exception( errStr, lException=error )
      return S_ERROR(errStr)

  types_getRequestNamesForJobs = [ ListType ]
  @staticmethod
  def export_getRequestNamesForJobs( jobIDs ):
    """ Select the request names for supplied jobIDs """
    gLogger.info( "getRequestNamesForJobs: Attempting to get request names for %s jobs." % len( jobIDs ) )
    try:
      return gRequestDB.getRequestNamesForJobs( jobIDs )
    except Exception, error:
      errStr = "getRequestNamesForJobs: Exception which getting request names."
      gLogger.exception( errStr, '', lException=error )
      return S_ERROR(errStr)
    
  types_readRequestsForJobs = [ ListType ]
  @staticmethod
  def export_readRequestsForJobs( jobIDs ):
    """ read requests for jobs given list of jobIDs """
    gLogger.verbose( "readRequestsForJobs: Attempting to read requests associated to the jobs." )
    try:
      res = gRequestDB.readRequestsForJobs( jobIDs )
      return res
    except Exception, error:
      errStr = "readRequestsForJobs: Exception while selecting requests."
      gLogger.exception( errStr, '', lException=error )
      return S_ERROR( errStr )

  types_getDigest = [ StringTypes ]
  @staticmethod
  def export_getDigest( requestName ):
    """ get digest for a request given its name

    :param str requestName: request's name
    :return: S_OK( json_str )
    """
    gLogger.verbose("RequestManagerHandler.getDigest: Attempting to get digest for request '%s'" % requestName )
    try:
      return gRequestDB.getDigest( requestName )
    except Exception , error:
      errStr = "RequestManagerHandler.getDigest: exception when getting digest for '%s'" % requestName 
      gLogger.exception( errStr, '', lException=error )
      return S_ERROR( errStr )
