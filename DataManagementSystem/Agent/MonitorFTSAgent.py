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
from DIRAC.RequestManagementSystem.Client.ReqClient import ReqClient
from DIRAC.RequestManagementSystem.Client.Operation import Operation
from DIRAC.RequestManagementSystem.Client.File import File
# # from Resources
from DIRAC.Resources.Storage.StorageElement import StorageElement

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

  # # SE cache
  __seCache = {}

  # # min threads
  MIN_THREADS = 1
  # # max threads
  MAX_THREADS = 10

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
      self.__requestClient = ReqClient()
    return self.__requestClient

  @classmethod
  def getSE( cls, seName ):
    """ keep se in cache"""
    if seName not in cls.__seCache:
      cls.__seCache[seName] = StorageElement( seName )
    return cls.__seCache[seName]

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
    # # placeholder for request
    request = None
    # # flag
    transferOpFinished = False

    if ftsJob.Status in FTSJob.FINALSTATES:

      # # perform getting full monitoring info
      monitor = ftsJob.monitorFTS2( full = True )
      if not monitor["OK"]:
        log.error( monitor["Message"] )
        return monitor

      # # split FTSFiles to different classes
      processFiles = self.filterFiles( ftsJob )
      if not processFiles["OK"]:
        log.error( processFiles["Message"] )
        return processFiles
      processFiles = processFiles["Value"]

      # # ... and keep them for further processing
      toReschedule = processFiles.get( "toReschedule", [] )
      toUpdate = processFiles.get( "toUpdate", [] )
      toRetry = processFiles.get( "toRetry", [] )
      toRegister = processFiles.get( "toRegister", [] )

      # # update ftsFiles to retry
      for ftsFile in toRetry:
        ftsFile.Status = "Waiting"
        putFile = self.ftsManager().putFile( ftsFile )
        if not putFile["OK"]:
          # # bail out - unable to put file back
          log.error( "unable to put file for retry: %s" % putFile["Message"] )
          ftsJob.Status = "Submitted"
          break

      opId = ftsJob[0].OperationID
      request = self.requestClient().getScheduledRequest( opId )
      if not request["OK"]:
        # # bailout - request can not be read
        log.error( request["Message"] )
        # # will retry later
        ftsJob.Status = "Submitted"
        break

      request = request["Value"]
      if not request:
        log.warn( "Request for FTSJob not found in ReqDB, probably deleted" )
        for ftsFile in ftsJob:
          ftsFile.Status = "Canceled"
          ftsFile.Error = "Request deleted"
        ftsJob.Status = "Canceled"
        break

      transferOp = None
      for op in request:
        if op.OperationID == opId:
          transferOp = op
          break

      missingReplicas = self.checkReadyReplicas( transferOp )
      if not missingReplicas["OK"]:
        # # bail out on error
        log.error( missingReplicas["Message"] )
        break
      missingReplicas = missingReplicas["Value"]

      if not missingReplicas:
        log.info( "all files replicated in 'ReplicateAndRegister' OperationID=%s Request '%s'" % ( opId,
                                                                                                   request.RequestName ) )
        for ftsFile in ftsJob:
          ftsFile.Status = "Finished"
        ftsJob.Status = "Finished"
        break

      if toReschedule:
        self.rescheduleFiles( transferOp, toReschedule )

      if toRegister:
        self.registerFiles( request, transferOp, toRegister )

      if toUpdate:
        update = self.ftsClient().setFTSFilesWaiting( opId,
                                                      ftsJob.TargetSE,
                                                      [ ftsFile.FileID for ftsFile in toUpdate ] )
        if not update["OK"]:
          log.error( "unable to update descendants for finished FTSFiles: %s" % update["Message"] )
          ftsJob.Status = "Submitted"
          break

    # # put back request if any
    if request:
      putRequest = self.requestClient().putRequest( request )
      if not putRequest["OK"]:
        log.error( "unable to put back request: %s" % putRequest["Message"] )
      return putRequest

    putFTSJob = self.ftsClient().putFTSJob( ftsJob )
    if not putFTSJob["OK"]:
      log.error( putFTSJob["Message"] )
      gMonitor.addMark( "FTSMonitorFail", 1 )
      return putFTSJob

    gMonitor.addMark( "FTSMonitorOK", 1 )
    return S_OK()

  def checkReadyReplicas( self, transferOperation ):
    """ check ready replicas for transferOperation """
    targetSESet = set( transferOperation.targetSEList )

    # # { LFN: [ targetSE, ... ] }
    missingReplicas = {}

    scheduledFiles = dict( [ ( opFile.LFN, opFile ) for opFile in transferOperation
                              if opFile.Status in ( "Scheduled", "Waiting" ) ] )
    # # get replicas
    replicas = self.replicaManager().getCatalogReplicas( scheduledFiles.keys() )

    if not replicas["OK"]:
      self.log.error( replicas["Message"] )
      return replicas
    replicas = replicas["Value"]

    for successfulLFN, reps in replicas["Successful"]:
      if targetSESet.issubset( set( reps ) ):
        scheduledFiles[successfulLFN].Status = "Done"
      else:
        missingReplicas[successfulLFN] = list( set( reps ) - targetSESet )

    reMissing = re.compile( "no such file or directory" )
    for failedLFN, errStr in replicas["Failed"]:
      scheduledFiles[failedLFN].Error = errStr
      if reMissing.search( errStr.lower() ):
        scheduledFiles[failedLFN].Status = "Failed"

    return S_OK( missingReplicas )

  def rescheduleFiles( self, operation = None, toReschedule = None ):
    """ update statues for Operation.Files to waiting """
    if not operation:
      return S_OK()
    toReschedule = toReschedule if toReschedule else []
    ids = [ ftsFile.FileID for ftsFile in toReschedule ]
    for opFile in operation:
      if opFile.FileID in ids:
        opFile.Status = "Waiting"
    return S_OK()

  def registerFiles( self, request = None, transferOp = None, toRegister = None ):
      """ add file registration """
      if not request or not transferOp:
        return S_OK()
      toRegister = toRegister if toRegister else []
      if toRegister:
        registerOp = Operation()
        registerOp.Type = "RegisterReplica"
        registerOp.Status = "Waiting"
        registerOp.TargetSE = toRegister[0].TargetSE
        targetSE = self.getSE( registerOp.TargetSE )
        for ftsFile in toRegister:
          opFile = File()
          opFile.LFN = ftsFile.LFN
          pfn = targetSE.getPfnForProtocol( ftsFile.TargetSURL, "SRM2", withPort = False )
          if not pfn["OK"]:
            continue
          opFile.PFN = pfn["Value"]
          registerOp.addFile( opFile )
        request.insertBefore( registerOp, transferOp )
      return S_OK()


  def filterFiles( self, ftsJob ):
    """ process ftsFiles from finished ftsJob  """

    toUpdate = []
    toReschedule = []
    toRegister = []
    toRetry = []

    # #  read request
    for ftsFile in ftsJob:
      # # successful files
      if ftsFile.Status == "Finished":
        if ftsFile.Error == "AddCatalogReplicaFailed":
          toRegister.append( ftsFile )
        toUpdate.append( ftsFile )
        continue
      if ftsFile.Status == "Failed":
        if ftsFile.Error == "MissingSource":
          toReschedule.append( ftsFile )
        else:
          toRetry.append( ftsFile )

    return S_OK( { "toUpdate": toUpdate,
                   "toRetry": toRetry,
                   "toRegister": toRegister,
                   "toReschedule": toReschedule } )

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
