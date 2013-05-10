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
# # from DIRAC
from DIRAC import S_OK, S_ERROR, gLogger
# # from Core
from DIRAC.Core.Utilities.ThreadPool import ThreadPool
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.Core.Utilities.List import getChunk
# # from DMS
from DIRAC.DataManagementSystem.Client.FTSClient import FTSClient
from DIRAC.DataManagementSystem.private.FTSStrategy import FTSGraph
from DIRAC.DataManagementSystem.Client.FTSJob import FTSJob
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

  def initialize( self ):
    """ agent's initialization """

    # # TODO: list indices must be integers not str
    ftsSites = self.ftsClient().getFTSSitesList()
    if not ftsSites["OK"]:
      self.log.error( "initialize: unable to get FTS sites list: %s" % ftsSites["Message"] )
      return ftsSites
    ftsSites = ftsSites["Value"]

    ftsHistory = self.ftsClient().getFTSHistory()
    if not ftsHistory["OK"]:
      self.log.error( "initialize: unable to get FTS history: %s" % ftsHistory["Message"] )
      return ftsHistory
    ftsHistory = ftsHistory["Value"]

    self.__ftsGraph = FTSGraph( "FTSGraph", ftsSites, ftsHistory )
    for i, ftsSite in enumerate( self.__ftsGraph.nodes() ):
      self.log.info( "[%d] FTSSite: %s ServerURI: %s" % ( i, ftsSite.name, ftsSite.ServerURI ) )

    if not self.__ftsGraph.nodes():
      self.log.error( "initialize: FTSSites not defined!!!" )
      return S_ERROR( "FTSSites not defined in FTSDB" )

    self.MAX_FILES_PER_JOB = self.am_getOption( "FilesPerJob", self.MAX_FILES_PER_JOB )
    self.log.info( "FTSFiles/FTSJob = %d" % self.MAX_FILES_PER_JOB )

    self.MIN_THREADS = self.am_getOption( "MinThreads", self.MIN_THREADS )
    self.MAX_THREADS = self.am_getOption( "MaxThreads", self.MAX_THREADS )
    minmax = ( abs( self.MIN_THREADS ), abs( self.MAX_THREADS ) )
    self.MIN_THREADS, self.MAX_THREADS = min( minmax ), max( minmax )
    self.log.info( "ThreadPool min threads = %s" % self.MIN_THREADS )
    self.log.info( "ThreadPool max threads = %s" % self.MAX_THREADS )

    self.__threadPool = ThreadPool( self.MIN_THREADS, self.MAX_THREADS )
    self.__threadPool.daemonize()

    # # read CS options
    self.MAX_JOBS_PER_ROUTE = self.am_getOption( "MaxJobsPerChannel", self.MAX_JOBS_PER_ROUTE )
    self.log.info( "max jobs/route = %s" % self.MAX_JOBS_PER_ROUTE )

    # This sets the Default Proxy to used as that defined under
    # /Operations/Shifter/DataManager
    # the shifterProxy option in the Configuration can be used to change this default.
    self.am_setOption( 'shifterProxy', 'DataManager' )
    return S_OK()

  def execute( self ):
    """ one cycle execution """
    self.log.info( "execute: updating RW for SEs..." )
    # # up[date RW access for SE first
    self.__ftsGraph.updateRWAccess()

    self.log.info( "execute: reading FTSFiles..." )
    ftsFileList = self.ftsClient().getFTSFileList( ["Waiting"] )
    if not ftsFileList["OK"]:
      self.log.error( "execute: unable to read Waiting FTSFiles: %s" % ftsFileList["Message"] )
      return ftsFileList
    ftsFileList = ftsFileList["Value"]

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
          sTJId = "[Thread-%d](%s %s)" % ( sourceSE, targetSE, enqueued )
          while True:
            queue = self.__threadPool.generateJobAndQueueIt( self.submitTransfer,
                                                             args = ( ftsFileListChunk, targetSite.ServiceURI,
                                                                      sourceSE, targetSE, sTJId ),
                                                             sTJId = sTJId )
            if queue["OK"]:
              self.log.info( "execute: enqueued transfer '%s'" % sTJId )
              enqueued += 1
              break
            time.sleep( 1 )

    # # process all results
    self.__threadPool.processAllResults()
    return S_OK()

  def submitTransfer( self, ftsFileList, ftsServerURI, sourceSE, targetSE, sTJId ):
    """ create and submit FTSJob

    :param list ftsFileList: list with FTSFiles
    :param str ftsServerURI: FTS server URI
    :param str sourceSE: source SE
    :param str targetSE: targetSE
    :param str sTJId: thread name for sublogger
    """
    log = gLogger.getSubLogger( sTJId )

    log.info( "got sourceSE=%s targetSE=%s ftsServer=%s ftsFiles=%s" % ( sourceSE, targetSE,
                                                                     ftsServerURI, len( ftsFileList ) ) )

    ftsJob = FTSJob()
    ftsJob.FTSServer = ftsServerURI
    ftsJob.SourceSE = sourceSE
    ftsJob.TargetSE = targetSE

    # # TODO: list of rejected files
    rejectedFiles = []
    for ftsFile in ftsFileList:
      # # TODO: check source file presence and its metadata
      ftsJob.addFile( ftsFile )



    self.log.info( "submitting..." )
    submit = ftsJob.submitFTS2()
    if not submit["OK"]:
      log.error( submit["Message"] )
      return submit
    log.info( "FTSJob %s submitted to FTS server %s" % ( ftsJob.FTSGUID, ftsJob.FTSServer ) )

    for ftsFile in ftsJob:
      ftsFile.Status = "Submitted"
      ftsFile.FTSGUID = ftsJob.FTSGUID

    putFTSJob = self.ftsClient().putFTSJob( ftsJob )
    if not putFTSJob["OK"]:
      log.error( putFTSJob["Message"] )
      return putFTSJob
    # # if we're here job was submitted and  saved
    return S_OK()
