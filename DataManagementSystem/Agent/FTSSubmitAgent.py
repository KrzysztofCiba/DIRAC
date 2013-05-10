########################################################################
# $HeadURL$
########################################################################
""" :mod: FTSSubmitAgent
    ====================

    .. module: FTSSubmitAgent
    :synopsis: agent submitting FTS jobs to the external FTS services
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    FTS Submit Agent takes files from the FTSDB and submits them to the FTS using
    FTSJob helper class.
"""
# # imports
import time
import datetime
import uuid
# # from DIRAC
from DIRAC import S_OK, S_ERROR, gLogger, gMonitor
# # from Core
from DIRAC.Core.Utilities.ThreadPool import ThreadPool
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.Core.Utilities.List import getChunk
# # from DMS
from DIRAC.DataManagementSystem.Client.FTSClient import FTSClient
from DIRAC.DataManagementSystem.Client.FTSJob import FTSJob
from DIRAC.DataManagementSystem.private.FTSGraph import FTSGraph
from DIRAC.DataManagementSystem.private.FTSHistoryView import FTSHistoryView
# # from RSS
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import Resources
from DIRAC.ResourceStatusSystem.Client.ResourceStatus import ResourceStatus

__RCSID__ = "$Id$"

class FTSSubmitAgent( AgentModule ):
  """
  .. class:: FTSSubmitAgent

  This class is submitting previously scheduled files to the FTS system using helper class FTSJob.

  Files to be transferred are read from FTSDB.FTSFile table, only those with Status = 'Waiting'.
  After submission FTSDB.FTSFile.Status is set to 'Submitted'. The rest of state propagation is
  done in FTSMonitorAgent.

  An information about newly created FTS jobs is hold in FTSDB.FTSJob.
  """
  # # fts graph refresh in seconds
  FTSGRAPH_REFRESH = FTSHistoryView.INTERVAL / 2
  # # SE R/W access refresh in seconds
  RW_REFRESH = 600
  # # placeholder for max job per channel
  MAX_JOBS_PER_ROUTE = 10
  # # min threads
  MIN_THREADS = 1
  # # max threads
  MAX_THREADS = 10
  # # files per job
  MAX_FILES_PER_JOB = 100
  # # placeholder fot FTS client
  __ftsClient = None
  # # placeholder for resources helper
  __resources = None
  # # placeholder for RSS client
  __rssClient = None
  # # placeholder for FTSGraph
  __ftsGraph = None
  # # graph regeneration time delta
  __ftsGraphValidStamp = None
  # # r/w access valid stamp
  __rwAccessValidStamp = None
  # # placeholder for threadPool
  __threadPool = None


  def ftsClient( self ):
    """ FTS client """
    if not self.__ftsClient:
      self.__ftsClient = FTSClient()
    return self.__ftsClient

  def rssClient( self ):
    """ RSS client getter """
    if not self.__rssClient:
      self.__rssClient = ResourceStatus()
    return self.__rssClient

  def resources( self ):
    """ resource helper getter """
    if not self.__resources:
      self.__resources = Resources()
    return self.__resources

  def threadPool( self ):
    """ thread pool getter """
    if not self.__threadPool:
      self.__threadPool = ThreadPool( self.MIN_THREADS, self.MAX_THREADS )
      self.__threadPool.daemonize()
    return self.__threadPool

  def resetFTSGraph( self ):
    """ create fts graph """

    ftsSites = self.ftsClient().getFTSSitesList()
    if not ftsSites["OK"]:
      self.log.error( "resetFTSGraph: unable to get FTS sites list: %s" % ftsSites["Message"] )
      return ftsSites
    ftsSites = ftsSites["Value"]
    if not ftsSites:
      self.log.error( "resetFTSGraph: FTSSites list is empty, no records in FTSDB.FTSSite table?" )
      return S_ERROR( "no FTSSites found" )

    ftsHistory = self.ftsClient().getFTSHistory()
    if not ftsHistory["OK"]:
      self.log.error( "resetFTSGraph: unable to get FTS history: %s" % ftsHistory["Message"] )
      return ftsHistory
    ftsHistory = ftsHistory["Value"]

    self.__ftsGraph = FTSGraph( "FTSGraph", ftsSites, ftsHistory )
    for i, ftsSite in enumerate( self.__ftsGraph.nodes() ):
      self.log.info( "[%d] FTSSite: %-25s ServerURI: %s" % ( i, ftsSite.name, ftsSite.ServerURI ) )

    # # save graph stamp
    self.__ftsGraphValidStamp = datetime.datetime.now() + datetime.timedelta( seconds = self.FTSGRAPH_REFRESH )

    # # refresh SE R/W access
    self.__ftsGraph.updateRWAccess()
    self.__rwAccessValidStamp = datetime.datetime.now() + datetime.timedelta( seconds = self.RW_REFRESH )

    return S_OK()

  def initialize( self ):
    """ agent's initialization """

    self.FTSGRAPH_REFRESH = self.am_getOption( "FTSGraphValidityPeriod", self.FTSGRAPH_REFRESH )
    self.log.info( "FTSGraph validity period       = %s s" % self.FTSGRAPH_REFRESH )
    self.RW_REFRESH = self.am_getOption( "RWAccessValidityPeriod", self.RW_REFRESH )
    self.log.info( "SEs R/W access validity period = %s s" % self.RW_REFRESH )

    self.MAX_JOBS_PER_ROUTE = self.am_getOption( "MaxJobsPerChannel", self.MAX_JOBS_PER_ROUTE )
    self.log.info( "Max FTSJobs/route              = %s" % self.MAX_JOBS_PER_ROUTE )
    self.MAX_FILES_PER_JOB = self.am_getOption( "MaxFilesPerJob", self.MAX_FILES_PER_JOB )
    self.log.info( "Max FTSFiles/FTSJob            = %d" % self.MAX_FILES_PER_JOB )

    # # thread pool
    self.MIN_THREADS = self.am_getOption( "MinThreads", self.MIN_THREADS )
    self.MAX_THREADS = self.am_getOption( "MaxThreads", self.MAX_THREADS )
    minmax = ( abs( self.MIN_THREADS ), abs( self.MAX_THREADS ) )
    self.MIN_THREADS, self.MAX_THREADS = min( minmax ), max( minmax )
    self.log.info( "ThreadPool min threads         = %s" % self.MIN_THREADS )
    self.log.info( "ThreadPool max threads         = %s" % self.MAX_THREADS )

    self.log.info( "initialize: creation of FTSGraph..." )
    createGraph = self.resetFTSGraph()
    if not createGraph["OK"]:
      self.log.error( "initialize: %s" % createGraph["Message"] )
      return createGraph


    # This sets the Default Proxy to used as that defined under
    # /Operations/Shifter/DataManager
    # the shifterProxy option in the Configuration can be used to change this default.
    self.am_setOption( 'shifterProxy', 'DataManager' )

    # # gMonitor stuff here
    gMonitor.registerActivity( "FTSJobsAtt", "FTSJob created",
                               "FTSSubmitAgent", "Created FTSJobs/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "FTSJobsOK", "FTSJobs submitted",
                               "FTSSubmitAgent", "Submitted FTSJobs/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "FTSJobsFail", "FTSJobs submissions failed",
                               "FTSSubmitAgent", "Failed FTSJobs/min", gMonitor.OP_SUM )

    gMonitor.registerActivity( "FTSFilesPerJob", "FTSFiles per FTSJob",
                               "FTSSubmitAgent", "Number of FTSFiles per FTSJob", gMonitor.OP_MEAN )
    gMonitor.registerActivity( "FTSSizePerJob", "Average FTSFiles size per FTSJob",
                               "FTSSubmitAgent", "Average submitted size per FTSJob", gMonitor.OP_MEAN )
    return S_OK()

  def execute( self ):
    """ one cycle execution """
    now = datetime.datetime.now()
    if now > self.__ftsGraphValidStamp:
      self.log.info( "execute: resetting FTS graph " )
      resetFTSGraph = self.resetFTSGraph()
      if not resetFTSGraph["OK"]:
        self.log.error( "execute: FTSGraph recreation error: %s" % resetFTSGraph["Message"] )
        return resetFTSGraph
    if now > self.__rwAccessValidStamp:
      self.log.info( "execute: updating R/W access for SEs" )
      self.__ftsGraph.updateRWAccess()

    self.log.info( "execute: reading FTSFiles..." )
    ftsFileList = self.ftsClient().getFTSFileList( ["Waiting"] )
    if not ftsFileList["OK"]:
      self.log.error( "execute: unable to read Waiting FTSFiles: %s" % ftsFileList["Message"] )
      return ftsFileList
    ftsFileList = ftsFileList["Value"]

    if not ftsFileList:
      self.log.info( "execute: no FTSFiles to submit" )
      return S_OK()

    # #  [sourceSE][targetSE] => list of files
    ftsFileDict = {}
    for ftsFile in ftsFileList:
      if ftsFile.SourceSE not in ftsFileDict:
        ftsFileDict[ftsFile.SourceSE] = {}
      if ftsFile.TargetSE not in ftsFileDict[ftsFile.SourceSE]:
        ftsFileDict[ftsFile.SourceSE][ftsFile.TargetSE] = []
      ftsFileDict[ftsFile.SourceSE][ftsFile.TargetSE].append( ftsFile )

    self.log.info( "execute: entering main loop..." )
    # # thread job counter
    enqueued = 1
    # # entering sourceSE, targetSE, ftsFile loop
    for sourceSE, targetDict in ftsFileDict.items():

      sourceSite = self.__ftsGraph.findSiteForSE( sourceSE )
      if not sourceSite["OK"]:
        self.log.error( "execute: unable to find source site for %s SE" % sourceSE )
        continue
      sourceSite = sourceSite["Value"]
      if not sourceSite.SEs[sourceSE]["read"]:
        self.log.error( "execute: source SE %s is banned for reading" % sourceSE )
        continue

      for targetSE, ftsFileList in targetDict.items():
        targetSite = self.__ftsGraph.findSiteForSE( targetSE )
        if not targetSite["OK"]:
          self.log.error( "execute: unable to find target site for %s SE" % targetSE )
          continue
        targetSite = targetSite["Value"]
        if not targetSite.SEs[targetSE]["write"]:
          self.log.error( "execute: target SE %s is banned for writing" % sourceSE )
          continue

        self.log.info( "execute: %s files waiting for transfer from %s to %s" % ( len( ftsFileList ), sourceSE, targetSE ) )

        for ftsFileListChunk in getChunk( ftsFileList, self.MAX_FILES_PER_JOB ):
          sTJId = "submit-%s/%s/%s" % ( enqueued, sourceSE, targetSE )
          while True:
            queue = self.threadPool().generateJobAndQueueIt( self.submit,
                                                             args = ( ftsFileListChunk, targetSite.ServerURI,
                                                                      sourceSE, targetSE, sTJId ),
                                                             sTJId = sTJId )
            if queue["OK"]:
              self.log.info( "execute: enqueued transfer '%s'" % sTJId )
              enqueued += 1
              gMonitor.addMark( "FTSJobsAtt", 1 )
              break
            time.sleep( 1 )

    # # process all results
    self.__threadPool.processAllResults()
    return S_OK()

  def submit( self, ftsFileList, ftsServerURI, sourceSE, targetSE, sTJId ):
    """ create and submit FTSJob

    :param list ftsFileList: list with FTSFiles
    :param str ftsServerURI: FTS server URI
    :param str sourceSE: source SE
    :param str targetSE: targetSE
    :param str sTJId: thread name for sublogger
    """
    log = gLogger.getSubLogger( sTJId, True )

    log.info( "got %s FTSFiles to submit to ftsServer=%s" % ( len( ftsFileList ), ftsServerURI ) )

    ftsJob = FTSJob()
    ftsJob.FTSServer = ftsServerURI
    ftsJob.SourceSE = sourceSE
    ftsJob.TargetSE = targetSE

    # # TODO: list of rejected files
    rejectedFiles = []
    for ftsFile in ftsFileList:
      # # TODO: check source file presence and its metadata
      ftsJob.addFile( ftsFile )

    log.debug( "submitting..." )
    submit = S_OK()  # ftsJob.submitFTS2()
    if not submit["OK"]:
      gMonitor.addMark( "FTSJobsFail", 1 )
      log.error( submit["Message"] )
      return submit
    # # TODO:replace, this is just for testing
    ftsJob.FTSGUID = str( uuid.uuid4() )

    # # save newly created FTSJob
    log.info( "FTSJob %s submitted to FTS server %s" % ( ftsJob.FTSGUID, ftsJob.FTSServer ) )
    for ftsFile in ftsJob:
      ftsFile.Status = "Submitted"
      ftsFile.FTSGUID = ftsJob.FTSGUID
      ftsFile.Attempt = ftsFile.Attempt + 1

    putFTSJob = self.ftsClient().putFTSJob( ftsJob )
    if not putFTSJob["OK"]:
      log.error( putFTSJob["Message"] )
      gMonitor.addMark( "FTSJobsFail", 1 )
      return putFTSJob
    # # if we're here job was submitted and  saved
    gMonitor.addMark( "FTSJobsOK", 1 )

    gMonitor.addMark( "FTSFilesPerJob", ftsJob.Files )
    gMonitor.addMark( "FTSSizePerJob", ftsJob.Size )

    return S_OK()
