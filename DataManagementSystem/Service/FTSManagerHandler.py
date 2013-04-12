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
from types import DictType, IntType, ListType, StringTypes
# # from DIRAC
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.DISET.RequestHandler import RequestHandler
# # from DMS
from DIRAC.DataManagementSystem.Client.FTSJob import FTSJob
from DIRAC.DataManagementSystem.Client.FTSJobFile import FTSJobFile
from DIRAC.DataManagementSystem.Client.FTSLfn import FTSLfn
from DIRAC.DataManagementSystem.private.StrategyHandler import StrategyHandler
from DIRAC.DataManagementSystem.private.FTSValidator import FTSValidator

# # global instance of FTSDB
gFTSDB = None

def initializeRequestManagerHandler( serviceInfo ):
  """ initialise handler """
  global gFTSDB
  from DIRAC.DataManagementSystem.DB.FTSDB import FTSDB
  gFTSDB = FTSDB()
  return S_OK()

########################################################################
class FTSManagerHandler( RequestHandler ):
  """
  .. class:: FTSManagerHandler

  """
  # # fts validator
  __ftsValidator = None
  # # fts scheduler
  __ftsScheduler = None

  @classmethod
  def ftsValidator( cls ):
    """ FTSValidator instance getter """
    if not cls.__ftsValidator:
      cls.__ftsValidator = FTSValidator()
    return cls.__ftsValidator

  types_ftsSchedule = [ StringTypes, ListType, StringTypes ]
  def export_ftsSchedule( self, LFN, targetSEs, strategy = None ):
    """ call FTS scheduler

    :param str LFN: lfn
    :param list targetSEs: target SEs
    :param str strategy: strategy to use
    """
    pass

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

