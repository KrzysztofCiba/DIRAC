########################################################################
# $HeadURL $
# File: RequestExecutingAgent.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2013/03/12 15:36:47
########################################################################

""" :mod: RequestExecutingAgent
    ===========================

    .. module: RequestExecutingAgent
    :synopsis: request executing agent
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    request processing agent
"""

__RCSID__ = "$Id $"

# #
# @file RequestExecutingAgent.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/03/12 15:36:56
# @brief Definition of RequestExecutingAgent class.

# # imports
import time
# # from DIRAC
from DIRAC import gMonitor, S_OK, S_ERROR
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.Core.Utilities.ProcessPool import ProcessPool
from DIRAC.RequestManagementSystem.Client.RequestClient import RequestClient
from DIRAC.RequestManagementSystem.private.RequestTask import RequestTask

# # agent name
AGENT_NAME = "RequestManagement/RequestExecutingAgent"

########################################################################
class RequestExecutingAgent( AgentModule ):
  """
  .. class:: RequestExecutingAgent

  request processing agent using ProcessPool and Operation handlers
  """
  # # process pool
  __processPool = None
  # # request cache
  __requestCache = {}
  # # requests/cycle
  __requestsPerCycle = 100
  # # minimal nb of subprocess running
  __minProcess = 2
  # # maximal nb of subprocess executed same time
  __maxProcess = 4
  # # ProcessPool queue size
  __queueSize = 20
  # # ProcessTask default timeout in seconds
  __taskTimeout = 900
  # # ProcessPool finalization timeout
  __poolTimeout = 900
  # # ProcessPool sleep time
  __poolSleep = 5
  # # placeholder for RequestClient instance
  __requestClient = None

  def __init__( self, *args, **kwargs ):
    """ c'tor """
    # # call base class ctor
    AgentModule.__init__( self, *args, **kwargs )
    # # ProcessPool related stuff
    self.__requestsPerCycle = self.am_getOption( "RequestsPerCycle", self.__requestsPerCycle )
    self.log.info( "requests/cycle = %d" % self.__requestsPerCycle )
    self.__minProcess = self.am_getOption( "MinProcess", self.__minProcess )
    self.log.info( "ProcessPool min process = %d" % self.__minProcess )
    self.__maxProcess = self.am_getOption( "MaxProcess", 4 )
    self.log.info( "ProcessPool max process = %d" % self.__maxProcess )
    self.__queueSize = self.am_getOption( "ProcessPoolQueueSize", self.__queueSize )
    self.log.info( "ProcessPool queue size = %d" % self.__queueSize )
    self.__poolTimeout = int( self.am_getOption( "ProcessPoolTimeout", self.__poolTimeout ) )
    self.log.info( "ProcessPool timeout = %d seconds" % self.__poolTimeout )
    self.__poolSleep = int( self.am_getOption( "ProcessPoolSleep", self.__poolSleep ) )
    self.log.info( "ProcessPool sleep time = %d seconds" % self.__poolSleep )
    self.__taskTimeout = int( self.am_getOption( "ProcessTaskTimeout", self.__taskTimeout ) )
    self.log.info( "ProcessTask timeout = %d seconds" % self.__taskTimeout )
    # #  RequestTask class def
    self.__requestTask = RequestTask
    # # operation handlers
    self.operationHandlers = self.am_getOption( "Operations",
                                                [ "DIRAC/DataManagementSystem/Agent/RequestOperations/ReplicateAndRegister",
                                                  "DIRAC/DataManagementSystem/Agent/RequestOperations/FTSScheduler",
                                                  "DIRAC/DataManagementSystem/Agent/RequestOperations/PutAndRegister",
                                                  "DIRAC/DataManagemnetSystem/Agent/RequestOperations/RemoveReplica",
                                                  "DIRAC/DataManagemnetSystem/Agent/RequestOperations/RemoveFile",
                                                  "DIRAC/DataManagemnetSystem/Agent/RequestOperations/RegisterFile",
                                                  "DIRAC/RequestManagementSystem/Agent/RequestOperations/ForwardDISET" ] )
    self.log.info( "Operation handlers: %s" % ",".join( self.operationHandlers ) )
    # # handlers dict
    self.handlersDict = dict()
    # # common monitor activity
    gMonitor.registerActivity( "Iteration", "Agent Loops",
                               "RequestExecutingAgent", "Loops/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "Execute", "Request Processed",
                               "RequestExecutingAgent", "Requests/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "Done", "Request Completed",
                               "RequestExecutingAgent", "Requests/min", gMonitor.OP_SUM )
    # # create request dict
    self.__requestCache = dict()

  def processPool( self ):
    """ facede for ProcessPool """
    if not self.__processPool:
      minProcess = max( 1, self.__minProcess )
      maxProcess = max( self.__minProcess, self.__maxProcess )
      queueSize = abs( self.__queueSize )
      self.log.info( "ProcessPool: minProcess = %d maxProcess = %d queueSize = %d" % ( minProcess,
                                                                                       maxProcess,
                                                                                       queueSize ) )
      self.__processPool = ProcessPool( minProcess,
                                        maxProcess,
                                        queueSize,
                                        poolCallback = self.resultCallback,
                                        poolExceptionCallback = self.exceptionCallback )
      self.__processPool.daemonize()
    return self.__processPool

  def requestClient( self ):
    """ RequestClient getter """
    if not self.__requestClient:
      self.__requestClient = RequestClient()
    return self.__requestClient

  def cleanCache( self, requestName = None ):
    """ delete request from requestCache

    :param str requestName: Request.RequestName
    """
    if requestName in self.__requestCache:
      del self.__requestCache[requestName]
    return S_OK()

  def cacheRequest( self, request ):
    """ put request into requestCache

    :param Request request: Request instance
    """
    self.__requestCache.setdefault( request.RequestName, request )
    return S_OK()

  def resetRequest( self, requestName ):
    """ put back :requestName: to RequestClient

    :param str requestName: request's name
    """
    if requestName in self.__requestCache:
      reset = self.requestClient().updateRequest( self.__requestCache[requestName] )
      if not reset["OK"]:
        return S_ERROR( "resetRequest: unable to reset request %s: %s" % ( requestName, reset["Message"] ) )
    return S_OK()

  def resetAllRequests( self ):
    """ put back all requests without callback called into requestClient

    :param self: self reference
    """
    self.log.info( "resetAllRequests: will put %s back requests" % len( self.__requestCache ) )
    for requestName, request in self.__requestCache.iteritems():
      reset = self.requestClient().updateRequest( request )
      if not reset["OK"]:
        self.log.error( "resetAllRequests: unable to reset request %s: %s" % ( requestName, reset["Message"] ) )
        continue
      self.log.debug( "resetAllRequests: request %s has been put back with its initial state" % requestName )
    return S_OK()

  def initialize( self ):
    """ initialize agent

    at the moment creates handlers dictionary
    """
    for opHandler in self.operationHandlers:
      handlerName = opHandler.split( "/" )[-1]
      self.handlersDict[ handlerName ] = opHandler
      self.log.info( "initialize: registered handler '%s' for operation '%s'" % ( opHandler, handlerName ) )
    if not self.handlersDict:
      self.log.error( "initialize: operation handlers not set, check configuration option 'Operations'!" )
      return S_ERROR( "Operation handlers not set!" )
    return S_OK()

  def execute( self ):
    """ read requests from RequestClient and enqueue them into ProcessPool """
    gMonitor.addMark( "Iteration", 1 )

    taskCounter = 0
    while taskCounter < self.__requestsPerCycle:
      self.log.info( "execute: " )
      getRequest = self.requestClient().getRequest()
      if not getRequest["OK"]:
        self.log.error( "execute: %s" % getRequest["Message"] )
        break
      if not getRequest["Value"]:
        self.log.info( "execute: not more waiting requests to process" )
        break
      # # OK, we've got you
      request = getRequest["Value"]
      taskID = request.RequestName
      # # save current request in cache
      self.cacheRequest( request )

      self.log.info( "processPool tasks idle = %s working = %s" % ( self.processPool().getNumIdleProcesses(),
                                                                    self.processPool().getNumWorkingProcesses() ) )
      while True:
        if not self.processPool().getFreeSlots():
          self.log.info( "No free slots available in processPool, will wait %d seconds to proceed" % self.__poolSleep )
          time.sleep( self.__poolSleep )
        else:
          self.log.info( "spawning task for request '%s'" % ( request.RequestName ) )
          enqueue = self.processPool().createAndQueueTask( self.__requestTask,
                                                           kwargs = { "requestXML" : request.toXML()["Value"],
                                                                      "handlersDict" : self.handlersDict },
                                                           taskID = taskID,
                                                           blocking = True,
                                                           usePoolCallbacks = True,
                                                           timeOut = self.__taskTimeout )
          if not enqueue["OK"]:
            self.log.error( enqueue["Message"] )
          else:
            self.log.info( "successfully enqueued task %s" % taskID )
            # # update monitor
            gMonitor.addMark( "Processed", 1 )
            # # update request counter
            taskCounter += 1
            # # task created, a little time kick to proceed
            time.sleep( 0.1 )
            break

    # # clean return
    return S_OK()

  def finalize( self ):
    """ agent finalization """
    if self.__processPool:
      self.processPool().finalize( timeout = self.__poolTimeout )
    self.resetAllRequests()
    return S_OK()

  def resultCallback( self, taskID, taskResult ):
    """ definition of request callback function

    :param str taskID: Reqiest.RequestName
    :param dict taskResult: task result S_OK/S_ERROR
    """
    self.log.info( "%s result callback" % taskID )

    if not taskResult["OK"]:
      self.log.error( "%s result callback: %s" % ( taskID, taskResult["Message"] ) )
      if taskResult["Message"] == "Timed out":
        self.resetRequest( taskID )
      self.cleanCache( taskID )
      return
    # # clean cache
    self.cleanCache( taskID )

    taskResult = taskResult["Value"]
    # # add monitoring info
    monitor = taskResult["monitor"] if "monitor" in taskResult else {}
    for mark, value in monitor.items():
      try:
        gMonitor.addMark( mark, value )
      except Exception, error:
        self.log.exception( str( error ) )

  def exceptionCallback( self, taskID, taskException ):
    """ definition of exception callback function

    :param str taskID: Request.RequestName
    :param Exception taskException: Exception instance
    """
    self.log.error( "%s exception callback" % taskID )
    self.log.error( taskException )
    self.resetRequest( taskID )

