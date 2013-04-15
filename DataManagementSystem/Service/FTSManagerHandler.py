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
from DIRAC.DataManagementSystem.Client.FTSFile import FTSFile
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

  @staticmethod
  def _ancestorSortKeys( tree, aKey = "Ancestor" ):
    """ sorting keys of replicationTree by its hopAncestor value

    replicationTree is a dict ( channelID : { ... }, (...) }

    :param self: self reference
    :param dict tree: replication tree  to sort
    :param str aKey: a key in value dict used to sort
    """
    if False in [ bool( aKey in v ) for v in tree.values() ]:
      return S_ERROR( "ancestorSortKeys: %s key in not present in all values" % aKey )
    # # put parents of all parents
    sortedKeys = [ k for k in tree if aKey in tree[k] and not tree[k][aKey] ]
    # # get children
    pairs = dict( [ ( k, v[aKey] ) for k, v in tree.items() if v[aKey] ] )
    while pairs:
      for key, ancestor in dict( pairs ).items():
        if key not in sortedKeys and ancestor in sortedKeys:
          sortedKeys.insert( sortedKeys.index( ancestor ), key )
          del pairs[key]
    # # need to reverse this one, as we're inserting child before its parent
    sortedKeys.reverse()
    if sorted( sortedKeys ) != sorted( tree.keys() ):
      return S_ERROR( "ancestorSortKeys: cannot sort, some keys are missing!" )
    return S_OK( sortedKeys )


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
    # # build ftsFiles instance


    ftsFile = FTSFile()
    for key in ( "LFN", "FileID", "OperationID", "Checksum", "ChecksumType", "Size" ):
      setattr( ftsFile, key, fileJSON.get( key ) )
    ftsFile.TargetSE = ",".join( targetSEs )
    ftsFile.Status = "Waiting"

    try:
      put = gFTSDB.putFTSFile( ftsFile )
      if not put["OK"]:
        gLogger.error( put["Message"] )
        return put
    except Exception, error:
      gLogger.exception( error )
      return S_ERROR( str( error ) )





  types_putFTSFile = [ StringTypes ]
  @classmethod
  def export_putFTSLfn( cls, ftsLfnXML ):
    """ put FTSLfn into FTSDB """
    ftsFile = FTSFile.fromXML( dumpToStr = True )
    if not ftsFile["OK"]:
      gLogger.error( ftsFile["Message"] )
      return ftsFile
    ftsFile = ftsFile["Value"]
    isValid = cls.ftsValdator().validate( ftsFile )
    if not isValid["OK"]:
      gLogger.error( isValid["Message"] )
      return isValid
    try:
      return gFTSDB.putFTSFile( ftsFile )
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
