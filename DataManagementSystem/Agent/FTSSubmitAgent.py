########################################################################
# $HeadURL$
########################################################################
""" :mod: FTSSubmitAgent
    ====================

    FTS Submit Agent takes files from the TransferDB and submits them to the FTS using
    FTSRequest helper class.


    TODO: change to use FTSDB and FTSClient
"""

# # imports
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.DataManagementSystem.Clinet.FTSClient import FTSClient
from DIRAC.DataManagementSystem.private.FTSStrategy import FTSGraph
from DIRAC.DataManagementSystem.Client.FTSJob import FTSJob
from DIRAC.DataManagementSystem.Client.FTSFile import FTSFile
from DIRAC.DataManagementSystem.Client.FTSSite import FTSSite
# # from RSS
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import Resources
from DIRAC.ResourceStatusSystem.Client.ResourceStatus import ResourceStatus

# from DIRAC.DataManagementSystem.DB.TransferDB import TransferDB
# from DIRAC.DataManagementSystem.Client.FTSRequest import FTSRequest

__RCSID__ = "$Id$"

class FTSSubmitAgent( AgentModule ):
  """
  .. class:: FTSSubmitAgent

  This class is submitting previously scheduled files to the FTS system using helper class FTSRequest.

  Files to be transferred are read from TransferDB.Channel table, only those with Status = 'Waiting'.
  After submission TransferDB.Channel.Status is set to 'Executing'. The rest of state propagation is
  done in FTSMonitorAgent.

  An information about newly created FTS request is hold in TransferDB.FTSReq (request itself) table and
  TransferDB.FileToFTS (files in request) and TransferDB.FileToCat (files to be registered, for
  failover only).
  """
  # # placeholder for max job per channel
  MAX_JOBS_PER_ROUTE = 10

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
    """ agent's initialization

    :param self: self reference
    """
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

    self.ftsGraph = FTSGraph( "FTSGraph", ftsSites, ftsHistory )
    for i, ftsSite in enumerate( self.ftsGraph.nodes() ):
      self.log.info( "[%d] FTSSite: %s ServerURI: %s" % ( i, ftsSite.name, ftsSite.ServerURI ) )

    if not self.ftsGraph.nodes():
      self.log.error( "initialize: FTSSites not defined!!!" )
      return S_ERROR( "FTSSites not defined in FTSDB" )

    # # read CS options
    self.MAX_JOBS_PER_ROUTE = self.am_getOption( "MaxJobsPerChannel", self.MAX_JOBS_PER_ROUTE )
    self.log.info( "max jobs/route = %s" % self.MAX_JOBS_PER_ROUTE )

    # This sets the Default Proxy to used as that defined under
    # /Operations/Shifter/DataManager
    # the shifterProxy option in the Configuration can be used to change this default.
    self.am_setOption( 'shifterProxy', 'DataManager' )
    return S_OK()

  def execute( self ):
    """ execution in one agent's cycle

    :param self: self reference
    """


    return S_OK()

    #########################################################################
    #  Obtain the eligible channels for submission.
    self.log.info( 'Obtaining channels eligible for submission.' )
    res = self.transferDB.selectChannelsForSubmission( self.maxJobsPerChannel )
    if not res['OK']:
      self.log.error( "Failed to retrieve channels for submission.", res['Message'] )
      return S_OK()
    elif not res['Value']:
      self.log.info( "FTSSubmitAgent. No channels eligible for submission." )
      return S_OK()
    channelDicts = res['Value']
    self.log.info( 'Found %s eligible channels.' % len( channelDicts ) )

    #########################################################################
    # Submit to all the eligible waiting channels.
    i = 1
    for channelDict in channelDicts:
      infoStr = "\n\n##################################################################################\n\n"
      infoStr = "%sStarting submission loop %s of %s\n\n" % ( infoStr, i, len( channelDicts ) )
      self.log.info( infoStr )
      res = self.submitTransfer( channelDict )
      i += 1
    return S_OK()

  def submitTransfer( self, channelDict ):
    """ create and submit FTS jobs based on information it gets from the DB

    :param self: self reference
    :param dict channelDict: dict with channel info as read from TransferDB.selectChannelsForSubmission
    """

    # Create the FTSRequest object for preparing the submission
    oFTSRequest = FTSRequest()
    channelID = channelDict['ChannelID']
    filesPerJob = channelDict['NumFiles']

    #########################################################################
    #  Obtain the first files in the selected channel.
    self.log.info( "FTSSubmitAgent.submitTransfer: Attempting to obtain files to transfer on channel %s" % channelID )
    res = self.transferDB.getFilesForChannel( channelID, 2 * filesPerJob )
    if not res['OK']:
      errStr = 'FTSSubmitAgent.%s' % res['Message']
      self.log.error( errStr )
      return S_OK()
    if not res['Value']:
      self.log.info( "FTSSubmitAgent.submitTransfer: No files to found for channel." )
      return S_OK()
    filesDict = res['Value']
    self.log.info( 'Obtained %s files for channel' % len( filesDict['Files'] ) )

    sourceSE = filesDict['SourceSE']
    oFTSRequest.setSourceSE( sourceSE )
    targetSE = filesDict['TargetSE']
    oFTSRequest.setTargetSE( targetSE )
    self.log.info( "FTSSubmitAgent.submitTransfer: Attempting to obtain files for %s to %s channel." % ( sourceSE,
                                                                                                         targetSE ) )
    files = filesDict['Files']

    # # enable/disable cksm test
    oFTSRequest.setCksmTest( self.cksmTest )
    if self.cksmType:
      oFTSRequest.setCksmType( self.cksmType )

    #########################################################################
    #  Populate the FTS Request with the files.
    self.log.info( 'Populating the FTS request with file information' )
    fileIDs = []
    totalSize = 0
    fileIDSizes = {}
    for fileMeta in files:
      lfn = fileMeta['LFN']
      oFTSRequest.setLFN( lfn )
      oFTSRequest.setSourceSURL( lfn, fileMeta['SourceSURL'] )
      oFTSRequest.setTargetSURL( lfn, fileMeta['TargetSURL'] )
      fileID = fileMeta['FileID']
      fileIDs.append( fileID )
      totalSize += fileMeta['Size']
      fileIDSizes[fileID] = fileMeta['Size']

    oFTSRequest.resolveSource()
    noSource = [ lfn for lfn, fileInfo in oFTSRequest.fileDict.items()
                     if fileInfo.get( "Status", "" ) == "Failed" and fileInfo.get( "Reason", "" ) in ( "No replica at SourceSE",
                                                                                                   "Source file does not exist" ) ]
    toReschedule = []
    for fileMeta in files:
      if fileMeta["LFN"] in noSource:
        toReschedule.append( fileMeta["FileID"] )

    if toReschedule:
      self.log.info( "Found %s files to reschedule" % len( toReschedule ) )
      for fileID in toReschedule:
        res = self.transferDB.setFileToReschedule( fileID )
        if not res["OK"]:
          self.log.error( "Failed to update Channel table for failed files.", res["Message"] )
        elif res["Value"] == "max reschedule attempt reached":
          self.log.error( "setting Channel status to 'Failed' : " % res["Value"] )
          res = self.transferDB.setFileChannelStatus( channelID, fileID, 'Failed' )
          if not res["OK"]:
            self.log.error( "Failed to update Channel table for failed files.", res["Message"] )

    #########################################################################
    #  Submit the FTS request and retrieve the FTS GUID/Server
    self.log.info( 'Submitting the FTS request' )
    res = oFTSRequest.submit()
    if not res['OK']:
      errStr = "FTSSubmitAgent.%s" % res['Message']
      self.log.error( errStr )
      self.log.info( 'Updating the Channel table for files to retry' )
      res = self.transferDB.resetFileChannelStatus( channelID, fileIDs )
      if not res['OK']:
        self.log.error( 'Failed to update the Channel table for file to retry.', res['Message'] )
      return S_ERROR( errStr )
    ftsGUID = res['Value']['ftsGUID']
    ftsServer = res['Value']['ftsServer']
    infoStr = """Submitted FTS Job:

              FTS Guid: %s
              FTS Server: %s
              ChannelID: %s
              SourceSE: %s
              TargetSE: %s
              Files: %s

""" % ( ftsGUID, ftsServer, str( channelID ), sourceSE, targetSE, str( len( files ) ) )
    self.log.info( infoStr )

    # # filter out skipped files
    failedFiles = oFTSRequest.getFailed()
    if not failedFiles["OK"]:
      self.log.warn( "Unable to read skipped LFNs." )
    failedFiles = failedFiles["Value"] if "Value" in failedFiles else []
    failedIDs = [ meta["FileID"] for meta in files if meta["LFN"] in failedFiles ]
    # # only submitted
    fileIDs = [ fileID for fileID in fileIDs if fileID not in failedIDs ]
    # # sub failed from total size
    totalSize -= sum( [ meta["Size"] for meta in files if meta["LFN"] in failedFiles ] )

    #########################################################################
    #  Insert the FTS Req details and add the number of files and size
    res = self.transferDB.insertFTSReq( ftsGUID, ftsServer, channelID )
    if not res['OK']:
      errStr = "FTSSubmitAgent.%s" % res['Message']
      self.log.error( errStr )
      return S_ERROR( errStr )
    ftsReqID = res['Value']
    self.log.info( 'Obtained FTS RequestID %s' % ftsReqID )
    res = self.transferDB.setFTSReqAttribute( ftsReqID, 'SourceSE', sourceSE )
    if not res['OK']:
      self.log.error( "Failed to set SourceSE for FTSRequest", res['Message'] )
    res = self.transferDB.setFTSReqAttribute( ftsReqID, 'TargetSE', targetSE )
    if not res['OK']:
      self.log.error( "Failed to set TargetSE for FTSRequest", res['Message'] )
    res = self.transferDB.setFTSReqAttribute( ftsReqID, 'NumberOfFiles', len( fileIDs ) )
    if not res['OK']:
      self.log.error( "Failed to set NumberOfFiles for FTSRequest", res['Message'] )
    res = self.transferDB.setFTSReqAttribute( ftsReqID, 'TotalSize', totalSize )
    if not res['OK']:
      self.log.error( "Failed to set TotalSize for FTSRequest", res['Message'] )

    #########################################################################
    #  Insert the submission event in the FTSReqLogging table
    event = 'Submitted'
    res = self.transferDB.addLoggingEvent( ftsReqID, event )
    if not res['OK']:
      errStr = "FTSSubmitAgent.%s" % res['Message']
      self.log.error( errStr )

    #########################################################################
    #  Insert the FileToFTS details and remove the files from the channel
    self.log.info( 'Setting the files as Executing in the Channel table' )
    res = self.transferDB.setChannelFilesExecuting( channelID, fileIDs )
    if not res['OK']:
      self.log.error( 'Failed to update the Channel tables for files.', res['Message'] )

    lfns = []
    fileToFTSFileAttributes = []
    for fileMeta in files:
      lfn = fileMeta['LFN']
      fileID = fileMeta['FileID']
      lfns.append( lfn )
      fileToFTSFileAttributes.append( ( fileID, fileIDSizes[fileID] ) )

    self.log.info( 'Populating the FileToFTS table with file information' )
    res = self.transferDB.setFTSReqFiles( ftsReqID, channelID, fileToFTSFileAttributes )
    if not res['OK']:
      self.log.error( 'Failed to populate the FileToFTS table with files.' )
