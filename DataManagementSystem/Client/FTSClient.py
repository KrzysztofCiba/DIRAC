########################################################################
# $HeadURL $
# File: FTSClient.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2013/04/08 14:29:43
########################################################################

""" :mod: FTSClient
    ===============

    .. module: FTSClient
    :synopsis: FTS client
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    FTS client
"""

__RCSID__ = "$Id $"

# #
# @file FTSClient.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/04/08 14:29:47
# @brief Definition of FTSClient class.

# # imports
from DIRAC import gLogger, S_OK
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.ConfigurationSystem.Client import PathFinder
from DIRAC.Core.Base.Client import Client
# # from RMS
from DIRAC.RequestManagementSystem.Client.RequestClient import RequestClient
# # from DMS
from DIRAC.DataManagementSystem.Client.FTSJob import FTSJob
from DIRAC.DataManagementSystem.Client.FTSFile import FTSFile
from DIRAC.DataManagementSystem.private.FTSHistoryView import FTSHistoryView
from DIRAC.DataManagementSystem.private.FTSValidator import FTSValidator

########################################################################
class FTSClient( Client ):
  """
  .. class:: FTSClient

  DISET client for FTS
  """
  # # placeholder for FTSValidator
  __ftsValidator = None
  # # placeholder for FTSManager
  __ftsManager = None
  # # placeholder for request manager
  __requestClient = None

  def __init__( self, useCertificates = False ):
    """c'tor

    :param self: self reference
    :param bool useCertificates: flag to enable/disable certificates
    """
    Client.__init__( self )
    self.log = gLogger.getSubLogger( "DataManagement/FTSClient" )
    self.setServer( "DataManagement/FTSManager" )

  @classmethod
  def ftsValidator( cls ):
    """ get FTSValidator instance """
    if not cls.__ftsValidator:
      cls.__ftsValidator = FTSValidator()
    return cls.__ftsValidator

  @classmethod
  def ftsManager( cls, timeout = 240 ):
    """ get FTSManager instance """
    if not cls.__ftsManager:
      url = PathFinder.getServiceURL( "DataManagement/FTSManager" )
      if not url:
        raise RuntimeError( "CS option DataManagement/FTSManager URL is not set!" )
      cls.__ftsManager = RPCClient( url, timeout = timeout )
    return cls.__ftsManager

  @classmethod
  def requestClient( cls ):
    """ request client getter """
    if not cls.__requestClient:
      cls.__requestClient = RequestClient()
    return cls.__requestClient

  def putFTSFile( self, ftsFile ):
    """ put FTSFile into FTSDB

    :param FTSFile ftsFile: FTSFile instance
    """
    isValid = self.ftsValidator().validate( ftsFile )
    if not isValid["OK"]:
      self.log.error( isValid["Message"] )
      return isValid
    ftsFileXML = ftsFile.toXML( dumpToStr = True )
    if not ftsFileXML["OK"]:
      self.log.error( ftsFileXML["Message"] )
      return ftsFileXML
    ftsFileXML = ftsFileXML["Value"]
    return self.ftsManager().putFTSFile( ftsFileXML )

  def getFTSFile( self, ftsFileID = None ):
    """ get FTSFile

    :param int fileID: FileID
    :param int ftsFileID: FTSFileID
    """
    getFile = self.ftsManager().getFTSFile( ftsFileID )
    if not getFile["OK"]:
      self.log.error( getFile["Message"] )
    # # de-serialize
    if getFile["Value"]:
      getFile = FTSFile.fromXML( getFile["Value"] )
      if not getFile["OK"]:
        self.log.error( getFile["Message"] )
    return getFile

  def deleteFTSFile( self, ftsFileID = None ):
    """ get FTSFile

    :param int ftsFileID: FTSFileID
    """
    deleteFile = self.ftsManager().deleteFTSFile( ftsFileID )
    if not deleteFile["OK"]:
      self.log.error( deleteFile["Message"] )
      return deleteFile
    return S_OK()

  def putFTSJob( self, ftsJob ):
    """ put FTSJob into FTSDB

    :param FTSJob ftsJob: FTSJob instance
    """
    isValid = self.ftsValidator().validate( ftsJob )
    if not isValid["OK"]:
      self.log.error( isValid["Message"] )
      return isValid
    ftsJobXML = ftsJob.toXML()
    if not ftsJobXML["OK"]:
      self.log.error( ftsJobXML["Message"] )
      return ftsJobXML
    return self.ftsManager().putFTSJob( ftsJobXML )

  def getFTSJob( self, ftsJobID ):
    """ get FTS job

    :param int ftsJobID: FTSJobID
    """
    getJob = self.ftsManager().getFTSJob( ftsJobID )
    if not getJob["OK"]:
      self.log.error( getJob["Message"] )
      return getJob
    # # de-serialize
    if getJob["Value"]:
      getJob = getJob["Value"]
      getJob = FTSJob.fromXML( getJob )
      if not getJob["OK"]:
        self.log.error( getJob["Message"] )
    return getJob

  def deleteFTSJob( self, ftsJobID ):
    """ delete FTSJob into FTSDB

    :param int ftsJob: FTSJobID
    """
    deleteJob = self.ftsManager().deleteFTSJob( ftsJobID )
    if not deleteJob["OK"]:
      self.log.error( deleteJob["Message"] )
    return deleteJob

  def getFTSJobIDs( self, statusList = [ "Submitted", "Ready", "Active" ] ):
    """ get list of FTSJobIDs for a given status list """
    ftsJobIDs = self.ftsManager().getFTSJobIDs( statusList )
    if not ftsJobIDs["OK"]:
      self.log.error( ftsJobIDs["Message"] )
    return ftsJobIDs

  def getFTSHistory( self ):
    """ get FTS history snapshot """
    getFTSHistory = self.ftsManager().getFTSHistory()
    if not getFTSHistory["OK"]:
      self.log.error( getFTSHistory["Message"] )
      return getFTSHistory
    getFTSHistory = getFTSHistory["Value"]
    history = []
    for ftsHistory in getFTSHistory:
      ftsHistory = FTSHistoryView( ftsHistory )
      if not ftsHistory["OK"]:
        return ftsHistory
      history.append( ftsHistory["Value"] )
    return S_OK( history )

  def ftsSchedule( self, opFile, sourceSEs, targetSEs ):
    """ schedule lfn for FTS job

    :param  File opFile: RMS File instance
    :param list sourceSEs: list of valid sources
    :param list targetSEs: list of target SEs
    """
    opFileJSON = opFile.toJSON()
    if not opFileJSON["OK"]:
      self.log.error( opFileJSON["Message"] )
      return opFileJSON
    return self.ftsManager().ftsSchedule( opFileJSON["Value"], sourceSEs, targetSEs )

