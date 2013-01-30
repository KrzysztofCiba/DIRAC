########################################################################
# $HeadURL $
# File: RequestMindHandler.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2013/01/30 11:51:54
########################################################################

""" :mod: RequestMindHandler 
    +=======================
 
    .. module: RequestMindHandler
    :synopsis: request mind handler
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    request mind handler
"""

__RCSID__ = "$Id $"

##
# @file RequestMindHandler.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/01/30 11:52:10
# @brief Definition of RequestMindHandler class.

## imports
from types import IntType, LongType, StringType
from DIRAC.Core.Base.ExecutorMindHandler import ExecutorMindHandler

########################################################################
class RequestMindHandler( ExecutorMindHandler ):
  """
  .. class:: RequestMindHandler
  
  """

  MSG_DEFINITIONS = { 'ExecuteOperation': { 'taskId': ( IntType, LongType ),
                                            'taskStub': StringType,
                                            'eType': StringType },
                      'TaskDone': { 'taskId': ( IntType, LongType ),
                                     'taskStub': StringType },
                      'TaskFreeze': { 'taskId': ( IntType, LongType ),
                                       'taskStub': StringType,
                                       'freezeTime': ( IntType, LongType ) },
                      'TaskError': { 'taskId': ( IntType, LongType ),
                                      'errorMsg': StringType,
                                      'taskStub': StringType,
                                      'eType': StringType},
                      'ExecutorError': { 'taskId': ( IntType, LongType ),
                                          'errorMsg': StringType,
                                          'eType': StringType } }

  @classmethod
  def __getRequest( cls, nb = 10 ):
    """ read requests out of RequestDB """
    getRequest = cls.__reqDB.getRequest()
    if not getRequest["OK"]:
      pass
    return getRequest
    
    

  @classmethod
  def initializeHandler( cls, serviceInfoDict ):
    try:
      from DIRAC.RequestManagementSystem.DB.RequestDB import RequestDB
      cls.__reqDB = ReqDB()
    except Exception, error:
      return S_ERROR( "Could not connect to RequestDB: %s" % str( error ) )
    #cls.setFailedOnTooFrozen( False )
    #cls.setFreezeOnFailedDispatch( False )
    #cls.setFreezeOnUnknownExecutor( False )
    #cls.setAllowedClients( "JobManager" )
    #JobState.checkDBAccess()
    #JobState.cleanTaskQueues()
    #period = cls.srv_getCSOption( "LoadJobPeriod", 60 )
    result = ThreadScheduler.gThreadScheduler.addPeriodicTask( period, cls.__getRequest )
    if not result["OK"]:
      return result
    #cls.__loadTaskId = result[ 'Value' ]
    return cls.__getRequest()

  @classmethod
  def exec_dispatch( cls, requestId, request, pathExecuted ):
    """ dispatcher

    """
    waitingOp = request.getWaiting()
    if waitingOp["OK"] and waitingOp["Value"]:
      opType = waitingOp["Value"].Type
      ## TODO - define look up dict
      ## TODO - dispatch using above
      if opType:
        pass

  @classmethod
  def exec_serializeTask( cls, request ):
    """ serialise request to xml

    :param Request request: request to be serialised
    """
    xmlStr = request.toXML()
    if not xmlStr["OK"]:
      return S_ERROR("exec_serialiseTask: unable to serialise request '%s' to xml" % request.RequestName )
    return S_OK( xmlStr["Value"] )
  
  @classmethod
  def exec_deserializeTask( cls, reqXML ):
    """ deserialise request from xml  

    :param str reqXML: xml string
    """
    request = Request.fromXML( reqXML )
    return S_OK( request )
  
  @classmethod
  def exec_taskError( cls, taskId, taskObj, errorMsg ):
    raise Exception( "No exec_taskError defined or it is not a classmethod!!" )
  
  @classmethod
  def exec_taskProcessed( cls, taskId, taskObj, eType ):
    raise Exception( "No exec_taskProcessed defined or it is not a classmethod!!" )
  
  @classmethod
  def exec_taskFreeze( cls, taskId, taskObj, eType ):
    return S_OK()
