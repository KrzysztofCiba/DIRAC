########################################################################
# $HeadURL $
# File: RequestAgent.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2013/03/12 15:36:47
########################################################################

""" :mod: RequestAgent 
    =======================
 
    .. module: RequestAgent
    :synopsis: request processing agent
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    request processing agent
"""

__RCSID__ = "$Id $"

##
# @file RequestAgent.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/03/12 15:36:56
# @brief Definition of RequestAgent class.

## imports 

## from DIRAC
from DIRAC import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.Core.Utilities.ProcessPool import ProcessPool
from DIRAC.RequestManagementSystem.Client.RequestClient import RequestClient
from DIRAC.RequestManagementSystem.Client.Request import Request

########################################################################
class RequestAgent( AgentModule ):
  """
  .. class:: RequestAgent

  request processing agent
  """
  ## process pool
  __processPool = None
  ## request cache
  __requestCache = {}  
  ## requests/cycle 
  __requestsPerCycle = 500
  ## minimal nb of subprocess running 
  __minProcess = 2
  ## maximal nb of subprocess executed same time
  __maxProcess = 4
  ## ProcessPool queue size 
  __queueSize = 20
  ## ProcessTask default timeout in seconds
  __taskTimeout = 300
  ## ProcessPool finalisation timeout 
  __poolTimeout = 300
  ## placeholder for RequestClient instance
  __requestClient = None

  def __init__( self, *args, **kwargs ):
    """ c'tor """
    AgentModule.__init__( self, *args, **kwargs )
    ## ProcessPool related stuff
    self.__requestsPerCycle = self.am_getOption( "RequestsPerCycle", self.__requestsPerCycle )
    self.log.info("requests/cycle = %d" % self.__requestsPerCycle )
    self.__minProcess = self.am_getOption( "MinProcess", self.__minProcess )
    self.log.info("ProcessPool min process = %d" % self.__minProcess )
    self.__maxProcess = self.am_getOption( "MaxProcess", 4 )
    self.log.info("ProcessPool max process = %d" % self.__maxProcess )
    self.__queueSize = self.am_getOption( "ProcessPoolQueueSize", self.__queueSize )
    self.log.info("ProcessPool queue size = %d" % self.__queueSize )
    self.__poolTimeout = int( self.am_getOption( "ProcessPoolTimeout", self.__poolTimeout ) )
    self.log.info("ProcessPool timeout = %d seconds" % self.__poolTimeout ) 
    self.__taskTimeout = int( self.am_getOption( "ProcessTaskTimeout", self.__taskTimeout ) )
    self.log.info("ProcessTask timeout = %d seconds" % self.__taskTimeout )
    ## shifter proxy
    self.am_setOption( "shifterProxy", "DataManager" )
    self.log.info( "Will use DataManager proxy." )
    ## common monitor activity 
    self.monitor.registerActivity( "Iteration", "Agent Loops", 
                                   self.__class__.__name__, "Loops/min", gMonitor.OP_SUM )
    self.monitor.registerActivity( "Execute", "Request Processed", 
                                   self.__class__.__name__, "Requests/min", gMonitor.OP_SUM )
    self.monitor.registerActivity( "Done", "Request Completed", 
                                   self.__class__.__name__, "Requests/min", gMonitor.OP_SUM )
    ## create request dict
    self.__requestCache = dict()

  def processPool( self ):
    """ facede for ProcessPool """
    if not self.__processPool:
      minProcess = max( 1, self.__minProcess ) 
      maxProcess = max( self.__minProcess, self.__maxProcess )
      queueSize = abs(self.__queueSize) 
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

  def cleanCache( self, requestName=None ):
    """ delete request from requestCache

    :param str requestName: Request.RequestName
    """
    if request.RequestName in self.__requestCache:
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
        return S_ERROR("resetRequest: unable to reset request %s: %s" % ( requestName, reset["Message"] ) )
    return S_OK()

  def resetAllRequests( self ):
    """ put back all requests without callback called into requestClient 

    :param self: self reference
    """
    self.log.info("resetAllRequests: will put %s back requests" % len(self.__requestCache) )
    for requestName, request in self.__requestCache.iteritems():
      reset = self.requestClient().updateRequest( request )
      if not reset["OK"]:
        self.log.error("resetAllRequests: unable to reset request %s: %s" % ( requestName, reset["Message"] ) )
        continue
      self.log.debug("resetAllRequests: request %s has been put back with its initial state" % requestName )
    return S_OK()

  def execute( self ):
    """ read requests from RequestClient and enqueue them into ProcessPool """
    taskCounter = 0
    while taskCounter < self.__requestsPerCycle:
      self.log.info("execute: ")
      getRequest = self.requestClient().getRequest()
      if not getRequest["OK"]:
        self.log.error("execute: %s" % getRequest["Message"] )
        break
      if not getRequest["Value"]:
        self.log.info("execute: not more waiting requests to process")
        break
      ## OK, we've got you
      request = getRequest["Value"]
      taskID = request.RequestName
      ## save curent request in cache  
      self.cacheRequest( request )

      self.log.info( "processPool tasks idle = %s working = %s" % ( self.processPool().getNumIdleProcesses(), 
                                                                    self.processPool().getNumWorkingProcesses() ) )
      while True:
        if not self.processPool().getFreeSlots():
          self.log.info("No free slots available in processPool, will wait 3 seconds to proceed...")
          time.sleep(3)
        else:
          self.log.info("spawning task for request '%s'" % ( request.RequestName ) )
          enqueue = self.processPool().createAndQueueTask( self.__requestTask, 
                                                           kwargs = { "XML" : request.toXML()["Value"] },
                                                           taskID = taskID,
                                                           blocking = True,
                                                           usePoolCallbacks = True,
                                                           timeOut = self.__taskTimeout )
          if not enqueue["OK"]:
            self.log.error( enqueue["Message"] )
          else:
            self.log.info("successfully enqueued task %s" % taskID )
            ## update request counter
            taskCounter += 1
            ## task created, a little time kick to proceed
            time.sleep( 0.1 )
            break

    ## clean return
    return S_OK()

  def finalize( self ):
    """ agent finalisation """
    if self.__processPool:
      self.processPool().finalize( timeout = self.__poolTimeout )
    self.resetAllRequests()
    return S_OK()

  def resultCallback( self, taskID, taskResult ):
    """ definition of request callback function
    
    :param str taskID: Reqiest.RequestName
    :param dict taskResult: task result S_OK/S_ERROR
    """
    self.log.info("%s result callback" %  taskID ) 

    if not taskResult["OK"]:
      self.log.error( "%s result callback: %s" % ( taskID, taskResult["Message"] ) )
      if taskResult["Message"] == "Timed out":
        self.resetRequest( taskID )
      self.cleanCache( taskID )
      return
    
    self.cleanCache( taskID )
    taskResult = taskResult["Value"]
    ## add monitoring info
    monitor = taskResult["monitor"] if "monitor" in taskResult else {}
    for mark, value in monitor.items():
      try:
        gMonitor.addMark( mark, value )
      except Exception, error:
        self.log.exception( str(error) )
    
  def exceptionCallback( self, taskID, taskException ):
    """ definition of exception callbak function
    
    :param str taskID: Request.RequestName
    :param Exception taskException: Exception instance
    """
    self.log.error( "%s exception callback" % taskID )
    self.log.error( taskException )
