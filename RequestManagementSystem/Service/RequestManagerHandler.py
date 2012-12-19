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
from types import DictType, IntType, ListType, LongType, StringTypes
## from DIRAC
from DIRAC import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.ConfigurationSystem.Client import PathFinder
## from RMS
from DIRAC.RequestManagementSystem.Client.Request import Request
from DIRAC.RequestManagementSystem.Client.Operation import Operation
from DIRAC.RequestManagementSystem.Client.File import File
from DIRAC.RequestManagementSystem.private.RequestValidator import RequestValidator

## global RequestDB instance
gRequestDB = None

def initializeRequestManagerHandler(serviceInfo):
  """ initialise handler """
  global gRequestDB
  csSection = PathFinder.getServiceSection( "RequestManagement/RequestManager" )
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

  types_setRequest = [ StringTypes ]
  @classmethod
  def export_putRequest( cls, requestString ):
    """ put a new request into RequestDB 

    :param cls: class ref
    :param str requestString: xml string
    """
    gLogger.info("RequestManager.setRequest: Setting request..." )
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
      return gRequestDB.setRequest( request )
    except Exception, error:
      errStr = "RequestManagerHandler.putRequest: Exception while setting request."
      gLogger.exception( errStr, requestName, lException=error )
      return S_ERROR(errStr)
    

  types_setRequestStatus = [ StringTypes, StringTypes ]
  @staticmethod
  def export_setRequestStatus( requestName, requestStatus ):
    """ Set status of a request """
    gLogger.info("RequestHandler.setRequestStatus: Setting status of %s to %s." % ( requestName, requestStatus ) )
    try:
      return gRequestDB.setRequestStatus( requestName, requestStatus )
    except Exception, error:
      errStr = "RequestHandler.setRequestStatus: Exception while setting request status."
      gLogger.exception( errStr, requestName, lException=error )
      return S_ERROR(errStr)

  types_updateRequest = [ StringTypes, StringTypes ]
  @staticmethod
  def export_updateRequest( requestName, requestString ):
    """ Update the request with the supplied string """
    gLogger.info("RequestManagerHandler.updateRequest: Attempting to update %s." % requestName)
    try:
      return gRequestDB.updateRequest( requestName, requestString )
    except Exception, error:
      errStr = "RequestManagerHandler.updateRequest: Exception which updating request."
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

  types_getRequest = []
  @staticmethod
  def export_getRequest():
    """ Get a request of given type from the database """
    gLogger.info("RequestHandler.getRequest: Attempting to get request")
    try:
      return gRequestDB.getRequest()
    except Exception, error:
      errStr = "RequestManagerHandler.getRequest: Exception while getting request."
      gLogger.exception( errStr, lException=error )
      return S_ERROR(errStr)

  types_getRequestSummaryWeb = [ DictType, ListType, IntType, IntType ]
  @staticmethod
  def export_getRequestSummaryWeb( selectDict, sortList, startItem, maxItems):
    """ Get summary of the request/subrequest info in the standard form for the web """
    return gRequestDB.getRequestSummaryWeb(selectDict, sortList, startItem, maxItems)
  
  types_getDistinctValues = [ StringTypes ]
  @staticmethod
  def export_getDistinctValues( attribute ):
    """ Get distinct values for a given (sub)request attribute """
    snames = ['RequestType', 'Operation', 'Status']
    rnames = ['OwnerDN', 'OwnerGroup']
    if attribute in snames:
      return gRequestDB.getDistinctAttributeValues('SubRequests', attribute)
    elif attribute in rnames:
      return gRequestDB.getDistinctAttributeValues('Requests', attribute)  
    return S_ERROR('Invalid attribute %s' % attribute)

  types_getDigest = [ list(StringTypes) + [ IntType, LongType ] ]
  def export_getDigest( self, requestName ):
    """ Get the digest of the request identified by its name """
    requestID = self.__getRequestID( requestName )
    return gRequestDB.getDigest( requestID["Value"] ) if requestID["OK"] else requestID
   
  types_getCurrentExecutionOrder = [ list(StringTypes) + [ IntType, LongType ] ]
  def export_getCurrentExecutionOrder( self, requestName ):
    """ Get the current execution order of the given request """
    requestID = self.__getRequestID( requestName )
    return gRequestDB.getCurrentExecutionOrder( requestID["Value"] ) if requestID["OK"] else requestID

  types_getRequestFileStatus = [ list(StringTypes) + [ IntType, LongType ], ListType ]
  def export_getRequestFileStatus( self, requestName, lfns ):
    """ Get the current status of the provided files for the request """
    requestID = self.__getRequestID( requestName )
    return gRequestDB.getRequestFileStatus( requestID["Value"], lfns ) if requestID["OK"] else requestID

  types_getRequestStatus = [ list(StringTypes) + [ IntType, LongType ] ]
  def export_getRequestStatus( self, requestName ):
    """ Get request status given :requestName: """
    requestID = self.__getRequestID( requestName )
    return gRequestDB.getRequestStatus( requestID["Value"] ) if requestID["OK"] else requestID

  types_getRequestInfo = [ list(StringTypes) + [ IntType, LongType ] ]
  def export_getRequestInfo( self, requestName ):
    """ get request info for given :requestName: """
    requestID = self.__getRequestID( requestName )
    return gRequestDB.getRequestInfo( requestID["Value"] ) if requestID["OK"] else requestID

  types_deleteRequest = [ StringTypes ]
  @staticmethod
  def export_deleteRequest( requestName ):
    """ Delete the request with the supplied name"""
    gLogger.info("RequestManagerHandler.deleteRequest: Deleting request '%s'..." % requestName)
    try:
      return gRequestDB.deleteRequest( requestName )
    except Exception, error:
      errStr = "RequestManagerHandler.deleteRequest: Exception which deleting request '%s'." % requestName 
      gLogger.exception( errStr, lException=error )
      return S_ERROR(errStr)

  types_getRequestForJobs = [ ListType ]
  @staticmethod
  def export_getRequestForJobs( jobIDs ):
    """ Select the request names for supplied jobIDs """
    gLogger.info("RequestManagerHandler.getRequestForJobs: Attempting to get request names for %s jobs." % len(jobIDs))
    try:
      return gRequestDB.getRequestForJobs( jobIDs )
    except Exception, error:
      errStr = "RequestManagerHandler.getRequestForJobs: Exception which getting request names."
      gLogger.exception( errStr, '', lException=error )
      return S_ERROR(errStr)
    
  types_selectRequests = [ DictType ]
  @staticmethod
  def export_selectRequests( selectDict, limit=100 ):
    """ Select requests according to supplied criteria """
    gLogger.verbose("RequestManagerHandler.selectRequests: Attempting to select requests." )
    try:
      return gRequestDB.selectRequests( selectDict, limit )
    except Exception, error:
      errStr = "RequestManagerHandler.selectRequests: Exception while selecting requests."
      gLogger.exception( errStr, '', lException=error)
      return S_ERROR(errStr)  

  types_readRequestsForJobs = [ ListType ]
  @staticmethod
  def export_readRequestsForJobs( jobIDs ):
    """ read requests for jobs given list of jobIDs """
    gLogger.verbose("RequestManagerHandler.readRequestsForJobs: Attepting to read requests associated to the jobs." )
    try:
      res = gRequestDB.readRequestsForJobs( jobIDs )
      return res
    except Exception, error:
      errStr = "RequestManagerHandler.readRequestsForJobs: Exception while selecting requests."
      gLogger.exception( errStr, '', lException=error )
      return S_ERROR( errStr )  

