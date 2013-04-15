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
from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.ConfigurationSystem.Client import PathFinder
from DIRAC.Core.Base.Client import Client
# # from RMS
from DIRAC.RequestManagementSystem.Client.RequestClient import RequestClient
# # from DMS
from DIRAC.DataManagementSystem.Client.FTSJob import FTSJob
from DIRAC.DataManagementSystem.Client.FTSFile import FTSFile
from DIRAC.DataManagementSystem.Client.FTSSite import FTSSite
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
    return self.ftsManager().putFTSFile( ftsFileXML )

  def getFTSFile( self, fileID = None, ftsFileID = None ):
    """ get FTSFile

    :param int fileID: FTSFileID
    """
    pass

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

  def putFTSSite( self, ftsSite ):
    """ put FTSSite into FTSDB

    :param FTSSite ftsSite: FTSSite instance
    """
    isValid = self.ftsValidator().validate( ftsSite )
    if not isValid["OK"]:
      self.log.error( isValid["Message"] )
      return isValid
    ftsSiteXML = ftsSite.toXML()
    if not ftsSiteXML["OK"]:
      self.log.error( ftsSiteXML["Message"] )
      return ftsSiteXML
    return self.ftsManager().putFTSSite( ftsSiteXML )

  def ftsSchedule( self, opFile, sourceSEs, targetSEs ):
    """ schedule lfn for FTS job

    :param  File opFile: RMS File instance
    :param list sourceSEs: list of valid sources
    :param list targetSEs: list of target SEs
    """
    opFileJSON = opFile.toJSON()
    return self.ftsManager().ftsSchedule( opFileJSON, sourceSEs, targetSEs )

