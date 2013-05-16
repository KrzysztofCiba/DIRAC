########################################################################
# $HeadURL$
########################################################################
"""
  :mod: MonitorFTSAgent
  =====================

  .. module: MonitorFTSAgent
  :synopsis: agent monitoring FTS jobs at the external FTS services
  .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

  The MonitorFTSAgent takes FTS jobs from the FTSDB and monitors their execution.
"""
# # imports
import time
import re
# # from DIRAC
from DIRAC import S_OK, S_ERROR, gLogger, gMonitor
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.Core.Utilities.ThreadPool import ThreadPool
# # from DMS
from DIRAC.DataManagementSystem.Client.FTSClient import FTSClient
from DIRAC.DataManagementSystem.Client.FTSJob import FTSJob
# # from RMS
from DIRAC.RequestManagementSystem.Client.RequestClient import RequestClient

# # RCSID
__RCSID__ = "$Id$"
# # agent's name

AGENT_NAME = 'DataManagement/MonitorFTSAgent'

class MonitorFTSAgent( AgentModule ):
  """
  .. class:: MonitorFTSAgent

  Monitor submitted FTS jobs.
  """
  # # FTS client
  __ftsClient = None
  # # thread pool
  __threadPool = None
  # # request client
  __requestClient = None

  # # min threads
  MIN_THREADS = 1
  # # max threads
  MAX_THREADS = 10

  # # missing source regexp patterns
  missingSourceErrors = [
    re.compile( r"SOURCE error during TRANSFER_PREPARATION phase: \[INVALID_PATH\] Failed" ),
    re.compile( r"SOURCE error during TRANSFER_PREPARATION phase: \[INVALID_PATH\] No such file or directory" ),
    re.compile( r"SOURCE error during PREPARATION phase: \[INVALID_PATH\] Failed" ),
    re.compile( r"SOURCE error during PREPARATION phase: \[INVALID_PATH\] The requested file either does not exist" ),
    re.compile( r"TRANSFER error during TRANSFER phase: \[INVALID_PATH\] the server sent an error response: 500 500"\
               " Command failed. : open error: No such file or directory" ),
    re.compile( r"SOURCE error during TRANSFER_PREPARATION phase: \[USER_ERROR\] source file doesnt exist" ) ]

  def ftsClient( self ):
    """ FTSClient getter """
    if not self.__ftsClient:
      self.__ftsClient = FTSClient()
    return self.__ftsClient

  def threadPool( self ):
    """ thread pool getter """
    if not self.__threadPool:
      self.__threadPool = ThreadPool( self.MIN_THREADS, self.MAX_THREADS )
      self.__threadPool.daemonize()
    return self.__threadPool

  def requestClient( self ):
    """ request client getter """
    if not self.__requestClient:
      self.__requestClient = RequestClient()
    return self.__requestClient

  @classmethod
  def missingSource( cls, failReason ):
    """ check if message sent by FTS server is concerning missing source file

    :param str failReason: message sent by FTS server
    """
    for error in cls.missingSourceErrors:
      if error.search( failReason ):
        return 1
    return 0


  def initialize( self ):
    """ agent's initialization """

    # # gMonitor stuff over here
    gMonitor.registerActivity( "FTSMonitorAtt", "FTSJobs monitor attempts",
                               "MonitorFTSAgent", "FTSJobs/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "FTSMonitorOK", "Successful FTSJobs monitor attempts",
                               "MonitorFTSAgent", "FTSJobs/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "FTSMonitorFail", "Failed FTSJobs monitor attempts",
                               "MonitorFTSAgent", "FTSJobs/min", gMonitor.OP_SUM )

    for status in list( FTSJob.INITSTATES + FTSJob.TRANSSTATES + FTSJob.FAILEDSTATES + FTSJob.FINALSTATES ):
      gMonitor.registerActivity( "FTSJobs%s" % status, "%s FTSJobs" % status ,
                                 "MonitorFTSAgent", "FTSJobs/min", gMonitor.OP_SUM )

    self.am_setOption( "shifterProxy", "DataManager" )

    self.MIN_THREADS = self.am_getOption( "MinThreads", self.MIN_THREADS )
    self.MAX_THREADS = self.am_getOption( "MaxThreads", self.MAX_THREADS )
    minmax = ( abs( self.MIN_THREADS ), abs( self.MAX_THREADS ) )
    self.MIN_THREADS, self.MAX_THREADS = min( minmax ), max( minmax )
    self.log.info( "ThreadPool min threads = %s" % self.MIN_THREADS )
    self.log.info( "ThreadPool max threads = %s" % self.MAX_THREADS )

    return S_OK()

  def execute( self ):
    """ push FTS Jobs to the thread pool """

    ftsJobs = self.ftsClient().getFTSJobList()
    if not ftsJobs["OK"]:
      self.log.error( "execute: failed to get FTSJobs: %s" % ftsJobs["Message"] )
      return ftsJobs

    ftsJobs = ftsJobs["Value"]

    if not ftsJobs:
      self.log.info( "execute: no active FTS jobs found." )
      return S_OK()

    self.log.info( "execute: found %s FTSJobs to monitor" % len( ftsJobs ) )

    enqueued = 1
    for ftsJob in ftsJobs:
      sTJId = "monitor-%s/%s" % ( enqueued, ftsJob.FTSJobID )
      while True:
        self.log.debug( "execute: submitting FTSJob %s to monitor" % ( ftsJob.FTSJobID ) )
        ret = self.threadPool().generateJobAndQueueIt( self.monitorTransfer, args = ( ftsJob, sTJId ), sTJId = sTJId )
        if ret["OK"]:
          gMonitor.addMark( "FTSMonitorAtt", 1 )
          enqueued += 1
          break
        # # sleep 1 second to proceed
        time.sleep( 1 )
    self.threadPool().processAllResults()
    return S_OK()

  def monitorTransfer( self, ftsJob, sTJId ):
    """ monitors transfer obtained from FTSDB

    :param dict ftsReqDict: FTS job dictionary
    """
    log = gLogger.getSubLogger( sTJId )

    log.info( "%s at %s" % ( ftsJob.FTSGUID, ftsJob.FTSServer ) )

    monitor = ftsJob.monitorFTS2()
    if not monitor["OK"]:
      gMonitor.addMark( "FTSMonitorFail", 1 )
      log.error( monitor["Message"] )
      if "getTransferJobSummary2: Not authorised to query request" in monitor["Message"]:
        log.error( "FTSJob expired at server" )
        return self.resetFiles( ftsJob, "FTSJob expired on server", sTJId )
      return monitor
    monitor = monitor["Value"]

    # # monitor status change
    gMonitor.addMark( "FTSJobs%s" % ftsJob.Status, 1 )

    if ftsJob.Status in FTSJob.FINALSTATES:
      processFiles = self.processFiles( ftsJob, sTJId )
      if not processFiles["OK"]:
        log.error( processFiles["Message"] )
        return processFiles

    putFTSJob = self.ftsClient().putFTSJob( ftsJob )
    if not putFTSJob["OK"]:
      log.error( putFTSJob["Message"] )
      gMonitor.addMark( "FTSMonitorFail", 1 )
      return putFTSJob

    gMonitor.addMark( "FTSMonitorOK", 1 )
    return S_OK()

  def processFiles( self, ftsJob, sTJId ):
    """ process ftsFiles from finished ftsJob  """
    log = gLogger.getSubLogger( "%s/processFiles" % sTJId )
    succFiles = []
    failFiles = []
    # #  read request
    for ftsFile in ftsJob:
      # # successful files
      if ftsFile.Status == "Finished":
        succFiles.append( ftsFile )
      else:
        failFiles.append( ftsFile )

    # if succFiles:
    #  for ftsFile in succFiles:
    #    self.ftsClient().getFTSFile()



  def resetFiles( self, ftsJob, reason, sTJId ):
    """ clean up when FTS job had expired on the server side

    :param FTSJob ftsJob: FTSJob instance
    """
    log = gLogger.getSubLogger( "%s/resetFiles" % sTJId )
    for ftsFile in ftsJob:
      ftsFile.Status = "Waiting"
      ftsFile.FTSGUID = ""
      ftsFile.Error = "FTSJob expired on server"
      putFile = self.ftsClient().putFTSFile( ftsFile )
      if not putFile["OK"]:
        log.error( putFile["Message"] )
        return putFile
    ftsJob.Status = "Failed"
    putJob = self.ftsClient().putFTSJob( ftsJob )
    if not putJob["OK"]:
      log.error( putJob["Message"] )
      return putJob
    return S_OK()


    #########################################################################
    # Update the information in the TransferDB if the transfer is terminal.
    res = oFTSRequest.isRequestTerminal()
    if not res["OK"]:
      log.error( "Failed to determine whether FTS request terminal", res["Message"] )
      return res
    if not res["Value"]:
      return S_OK()
    # # request is terminal
    return self.terminalRequest( oFTSRequest, ftsReqID, channelID, sourceSE )

  def terminalRequest( self, oFTSRequest, ftsReqID, channelID, sourceSE ):
    """ process terminal FTS job

    :param FTSRequest oFTSRequest: FTSRequest instance
    :param int ftsReqID: FTSReq.FTSReqID
    :param int channelID: FTSReq.ChannelID
    :param str sourceSE: FTSReq.SourceSE
    """
    log = gLogger.getSubLogger( "@%s" % ftsReqID )

    log.info( "FTS Request found to be terminal, updating file states" )
    #########################################################################
    # Get the LFNS associated to the FTS request
    log.info( "Obtaining the LFNs associated to this request" )
    res = self.transferDB.getFTSReqLFNs( ftsReqID, channelID, sourceSE )
    if not res["OK"]:
      log.error( "Failed to obtain FTS request LFNs", res['Message'] )
      return res
    files = res["Value"]
    if not files:
      log.error( "No files present for transfer" )
      return S_ERROR( "No files were found in the DB" )

    lfns = files.keys()
    log.debug( "Obtained %s files" % len( lfns ) )
    for lfn in lfns:
      oFTSRequest.setLFN( lfn )

    res = oFTSRequest.monitor()
    if not res["OK"]:
      log.error( "Failed to perform detailed monitoring of FTS request", res["Message"] )
      return res
    res = oFTSRequest.getFailed()
    if not res["OK"]:
      log.error( "Failed to obtained failed files for FTS request", res["Message"] )
      return res
    failedFiles = res["Value"]
    res = oFTSRequest.getDone()
    if not res["OK"]:
      log.error( "Failed to obtained successful files for FTS request", res["Message"] )
      return res
    completedFiles = res["Value"]

    # An LFN can be included more than once if it was entered into more than one Request.
    # FTS will only do the transfer once. We need to identify all FileIDs
    res = self.transferDB.getFTSReqFileIDs( ftsReqID )
    if not res["OK"]:
      log.error( "Failed to get FileIDs associated to FTS Request", res["Message"] )
      return res
    fileIDs = res["Value"]
    res = self.transferDB.getAttributesForFilesList( fileIDs, ["LFN"] )
    if not res["OK"]:
      log.error( "Failed to get LFNs associated to FTS Request", res["Message"] )
      return res
    fileIDDict = res["Value"]

    fileToFTSUpdates = []
    completedFileIDs = []
    filesToRetry = []
    filesToFail = []

    for fileID, fileDict in fileIDDict.items():
      lfn = fileDict['LFN']
      if lfn in completedFiles:
        completedFileIDs.append( fileID )
        transferTime = 0
        res = oFTSRequest.getTransferTime( lfn )
        if res["OK"]:
          transferTime = res["Value"]
        fileToFTSUpdates.append( ( fileID, "Completed", "", 0, transferTime ) )

      if lfn in failedFiles:
        failReason = ""
        res = oFTSRequest.getFailReason( lfn )
        if res["OK"]:
          failReason = res["Value"]
        if "Source file/user checksum mismatch" in failReason:
          filesToFail.append( fileID )
          continue
        if self.missingSource( failReason ):
          log.error( "The source SURL does not exist.", "%s %s" % ( lfn, oFTSRequest.getSourceSURL( lfn ) ) )
          filesToFail.append( fileID )
        else:
          filesToRetry.append( fileID )
        log.error( "Failed to replicate file on channel.", "%s %s" % ( channelID, failReason ) )
        fileToFTSUpdates.append( ( fileID, "Failed", failReason, 0, 0 ) )

    # # update TransferDB.FileToFTS table
    updateFileToFTS = self.updateFileToFTS( ftsReqID, channelID,
                                            filesToRetry, filesToFail,
                                            completedFileIDs, fileToFTSUpdates )

    if updateFileToFTS["OK"] and updateFileToFTS["Value"]:
      res = oFTSRequest.finalize()
      if not res["OK"]:
        log.error( "Failed to perform the finalization for the FTS request", res["Message"] )
        return res

      log.info( 'Adding logging event for FTS request' )
      # Now set the FTSReq status to terminal so that it is not monitored again
      res = self.transferDB.addLoggingEvent( ftsReqID, 'Finished' )
      if not res['OK']:
        log.error( 'Failed to add logging event for FTS Request', res['Message'] )

      # update TransferDB.FileToCat table
      updateFileToCat = self.updateFileToCat( oFTSRequest, channelID, fileIDDict, completedFiles, filesToFail )
      if not updateFileToCat["OK"]:
        log.error( updateFileToCat["Message"] )

      log.debug( "Updating FTS request status" )
      res = self.transferDB.setFTSReqStatus( ftsReqID, 'Finished' )
      if not res['OK']:
        log.error( 'Failed update FTS Request status', res['Message'] )
    return S_OK()


  def updateFileToFTS( self, ftsReqID, channelID, filesToRetry, filesToFail, completedFileIDs, fileToFTSUpdates ):
    """ update TransferDB.FileToFTS table for finished request

    :param int ftsReqID: FTSReq.FTSReqID
    :param int channelID: FTSReq.ChannelID
    :param list filesToRetry: FileIDs to retry
    :param list filesToFail: FileIDs for failed files
    :param list completedFileIDs: files completed
    :param list fileToFTSUpdates: ???
    """
    log = gLogger.getSubLogger( "@%s" % ftsReqID )

    allUpdated = True

    res = self.transferDB.resetFileChannelStatus( channelID, filesToRetry ) if filesToRetry else S_OK()
    if not res["OK"]:
      log.error( "Failed to update the Channel table for file to retry.", res["Message"] )
      allUpdated = False

    for fileID in filesToFail:
      log.info( "Updating the Channel table for files to reschedule" )
      res = self.transferDB.setFileToReschedule( fileID )
      if not res["OK"]:
        log.error( "Failed to update Channel table for failed files.", res["Message"] )
        allUpdated = False
      elif res["Value"] == "max reschedule attempt reached":
        log.error( "setting Channel status to 'Failed' : " % res["Value"] )
        res = self.transferDB.setFileChannelStatus( channelID, fileID, 'Failed' )
        if not res["OK"]:
          log.error( "Failed to update Channel table for failed files.", res["Message"] )
          allUpdated = False

    if completedFileIDs:
      res = self.transferDB.updateCompletedChannelStatus( channelID, completedFileIDs )
      if not res["OK"]:
        log.error( "Failed to update the Channel table for successful files.", res["Message"] )
        allUpdated = False
      res = self.transferDB.updateAncestorChannelStatus( channelID, completedFileIDs )
      if not res["OK"]:
        log.error( 'Failed to update the Channel table for ancestors of successful files.', res['Message'] )
        allUpdated = False

    if fileToFTSUpdates:
      res = self.transferDB.setFileToFTSFileAttributes( ftsReqID, channelID, fileToFTSUpdates )
      if not res["OK"]:
        log.error( "Failed to update the FileToFTS table for files.", res["Message"] )
        allUpdated = False

    return S_OK( allUpdated )

  def updateFileToCat( self, oFTSRequest, channelID, fileIDDict, completedFiles, filesToFail ):
    """ update TransferDB.FileToCat table for finished request

    :param FTSRequest oFTSRequest: FTSRequest instance
    :param int ftsReqID: FTSReq.FTSReqID
    :param dict fileIDDict: fileIDs dictionary
    :param int channelID: FTSReq.ChannelID
    """
    res = oFTSRequest.getFailedRegistrations()
    failedRegistrations = res["Value"]
    regFailedFileIDs = []
    regDoneFileIDs = []
    regForgetFileIDs = []
    for fileID, fileDict in fileIDDict.items():
      lfn = fileDict['LFN']

      if lfn in failedRegistrations:
        regFailedFileIDs.append( fileID )
        # if the LFN appears more than once, FileToCat needs to be reset only once
        del failedRegistrations[lfn]
      elif lfn in completedFiles:
        regDoneFileIDs.append( fileID )
      elif fileID in filesToFail:
        regForgetFileIDs.append( fileID )

    res = self.transferDB.setRegistrationWaiting( channelID, regFailedFileIDs ) if regFailedFileIDs else S_OK()
    if not res["OK"]:
      res["Message"] = "Failed to reset entries in FileToCat: %s" % res["Message"]
      return res

    res = self.transferDB.setRegistrationDone( channelID, regDoneFileIDs ) if regDoneFileIDs else S_OK()
    if not res["OK"]:
      res["Message"] = "Failed to set entries Done in FileToCat: %s" % res["Message"]
      return res

    # This entries could also be set to Failed, but currently there is no method to do so.
    res = self.transferDB.setRegistrationDone( channelID, regForgetFileIDs ) if regForgetFileIDs else S_OK()
    if not res["OK"]:
      res["Message"] = "Failed to set entries Done in FileToCat: %s" % res["Message"]
      return res

    return S_OK()
