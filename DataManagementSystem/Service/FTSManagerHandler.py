########################################################################
# $HeadURL $
# File: FTSManagerHandler.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2013/04/08 14:24:08
########################################################################

""" :mod: FTSManagerHandler
    =======================

    .. module: FTSManagerHandler
    :synopsis: handler for FTSDB using DISET
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    service handler for FTSDB using DISET
"""

__RCSID__ = "$Id $"

# #
# @file FTSManagerHandler.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/04/08 14:24:30
# @brief Definition of FTSManagerHandler class.

# # imports
# # imports
from types import DictType, LongType, ListType, StringTypes
# # from DIRAC
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.ConfigurationSystem.Client.PathFinder import getServiceSection
# # from DMS
from DIRAC.DataManagementSystem.Client.FTSJob import FTSJob
from DIRAC.DataManagementSystem.Client.FTSJobFile import FTSJobFile
from DIRAC.DataManagementSystem.Client.FTSLfn import FTSLfn
from DIRAC.DataManagementSystem.private.FTSStrategy import FTSStrategy
from DIRAC.DataManagementSystem.private.FTSValidator import FTSValidator

# # global instance of FTSDB
gFTSDB = None
gFTSStrategy = None

def initializeFTSManagerHandler( serviceInfo ):
  """ initialize handler """
  global gFTSDB
  global gFTSStrategy

  # # create FTSDB
  from DIRAC.DataManagementSystem.DB.FTSDB import FTSDB
  gFTSDB = FTSDB()

  # # create FTSStrategy when needed
  ftsMode = FTSManagerHandler.svr_getCSOption( "FTSMode", False )
  gLogger.info( "FTS is %s" % { True: "enabled", False: "disabled"}[ftsMode] )

  if ftsMode:
    csPath = getServiceSection( "DataManagement/FTSManager" )
    if not csPath["OK"]:
      gLogger.error( csPath["Message"] )
      return csPath
    csPath = "%s/%s" % ( csPath["Value"], "FTSStrategy" )
    gFTSStrategy = FTSStrategy( csPath )

  return S_OK()

########################################################################
class FTSManagerHandler( RequestHandler ):
  """
  .. class:: FTSManagerHandler

  """
  # # fts validator
  __ftsValidator = None

  @classmethod
  def ftsValidator( cls ):
    """ FTSValidator instance getter """
    if not cls.__ftsValidator:
      cls.__ftsValidator = FTSValidator()
    return cls.__ftsValidator

  types_ftsSchedule = [ DictType, ListType, ListType ]
  def export_ftsSchedule( self, fileJSON, sourceSEs, targetSEs ):
    """ call FTS scheduler

    :param str LFN: lfn
    :param list sourceSEs: source SEs
    :param list targetSEs: target SEs
    """
    if not gFTSStrategy:
      errMsg = "FTS mode is disabled or FTSStrategy could not be created"
      gLogger.error( errMsg )
      return S_ERROR( errMsg )
    size = fileJSON.get( "Size", 0 )
    tree = gFTSStrategy.replicationTree( sourceSEs, targetSEs, size )
    if not tree["OK"]:
      return tree
    tree = tree["Value"]
    # # build ftsLfn instance
    ftsLfn = FTSLfn()
    for key in ( "LFN", "FileID", "OperationID", "Checksum", "ChecksumType", "Size" ):
      setattr( ftsLfn, key, fileJSON.get( key ) )
    ftsLfn.TargetSE = ",".join( targetSEs )
    ftsLfn.Status = "Waiting"
    
    try:
      put = gFTSDB.putFTSLfn( ftsLfn )
    except Exception, error:
      gLogger.exception( error )
      return S_ERROR( str( error ) )
    
    for branch in tree:
      ftsJobFile = FTSJobFile()
      




  types_putFTSLfn = [ StringTypes ]
  @classmethod
  def export_putFTSLfn( cls, ftsLfnXML ):
    """ put FTSLfn into FTSDB """
    ftsLfn = FTSLfn.fromXML()
    if not ftsLfn["OK"]:
      gLogger.error( ftsLfn["Message"] )
      return ftsLfn
    ftsLfn = ftsLfn["Value"]
    isValid = cls.ftsValdator().validate( ftsLfn )
    if not isValid["OK"]:
      gLogger.error( isValid["Message"] )
      return isValid
    try:
      return gFTSDB.putFTSLfn( ftsLfn["Value"] )
    except Exception, error:
      gLogger.exception( error )
      return S_ERROR( error )

  types_putFTSJob = [ StringTypes ]
  @classmethod
  def export_putFTSJob( cls, ftsJobXML ):
    """ put FTSLfn into FTSDB """
    ftsJob = FTSJob.fromXML()
    if not ftsJob["OK"]:
      gLogger.error( ftsJob["Message"] )
      return ftsJob
    ftsJob = ftsJob["Value"]
    isValid = cls.ftsValdator().validate( ftsJob )
    if not isValid["OK"]:
      gLogger.error( isValid["Message"] )
      return isValid
    try:
      return gFTSDB.putFTSJob( ftsJob )
    except Exception, error:
      gLogger.exception( error )
      return S_ERROR( error )

