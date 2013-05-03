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
# # from Resources
from DIRAC.Resources.Storage.StorageFactory import StorageFactory
# # from DMS
from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
from DIRAC.DataManagementSystem.Client.FTSJob import FTSJob
from DIRAC.DataManagementSystem.Client.FTSFile import FTSFile
from DIRAC.DataManagementSystem.private.FTSHistoryView import FTSHistoryView
from DIRAC.DataManagementSystem.private.FTSStrategy import FTSStrategy
from DIRAC.DataManagementSystem.private.FTSValidator import FTSValidator

# # global instance of FTSDB
gFTSDB = None
gFTSStrategy = None

# def initializeFTSManagerHandler( serviceInfo ):
#  """ initialize handler """
#  global gFTSDB
#  global gFTSStrategy
#
#  # # create FTSDB
#  from DIRAC.DataManagementSystem.DB.FTSDB import FTSDB
#  gFTSDB = FTSDB()

#  # # create FTSStrategy when needed
#  #ftsMode = FTSManagerHandler.svr_getCSOption( "FTSMode", False )
#  #gLogger.info( "FTS is %s" % { True: "enabled", False: "disabled"}[ftsMode] )

#  #if ftsMode:
#  #  csPath = getServiceSection( "DataManagement/FTSManager" )
#  #  if not csPath["OK"]:
#  #    gLogger.error( csPath["Message"] )
# #    return csPath
#  #  csPath = "%s/%s" % ( csPath["Value"], "FTSStrategy" )
#  #  gFTSStrategy = FTSStrategy( csPath )

#  return S_OK()

########################################################################
class FTSManagerHandler( RequestHandler ):
  """
  .. class:: FTSManagerHandler

  """
  # # fts validator
  __ftsValidator = None
  # # storage factory
  __storageFactory = None
  # # replica manager
  __replicaManager = None


  @classmethod
  def initializeHandler( cls, serviceInfoDict ):
    """ initialize handler """
    global gFTSDB
    global gFTSStrategy
    from DIRAC.DataManagementSystem.DB.FTSDB import FTSDB
    gFTSDB = FTSDB()

    cls.ftsMode = cls.srv_getCSOption( "FTSMode", False )
    gLogger.always( "FTS is %s" % { True: "enabled", False: "disabled"}[cls.ftsMode] )

    if cls.ftsMode:
      csPath = getServiceSection( "DataManagement/FTSManager" )
      if not csPath["OK"]:
        gLogger.error( csPath["Message"] )
        return csPath
      csPath = "%s/%s" % ( csPath["Value"], "FTSStrategy" )
      gFTSStrategy = FTSStrategy( csPath )
    return S_OK()


  @classmethod
  def ftsValidator( cls ):
    """ FTSValidator instance getter """
    if not cls.__ftsValidator:
      cls.__ftsValidator = FTSValidator()
    return cls.__ftsValidator

  @classmethod
  def storageFactory( cls ):
    """ StorageFactory instance getter """
    if not cls.__storageFactory:
      cls.__storageFactory = StorageFactory()
    return cls.__storageFactory

  @classmethod
  def replicaManager( cls ):
    """ ReplicaManager instance getter """
    if not cls.__replicaManager:
      cls.__replicaManager = ReplicaManager()
    return cls.__replicaManager


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
    # # sort by ancestor
    sortedKeys = self._ancestorSortKeys( tree, "Ancestor" )
    if not sortedKeys["OK"]:
      gLogger.warn( "unable to sort replication tree by Ancestor: %s" % sortedKeys["Message"] )
      sortedKeys = tree.keys()
    else:
      sortedKeys = sortedKeys["Value"]
    # # dict holding swap parent with child for same SURLs

    ancestorSwap = {}
    for channelID in sortedKeys:
      repDict = tree[channelID]
      gLogger.info( "Strategy=%s Ancestor=%s SourceSE=%s TargetSE=%s" % ( repDict["Strategy"], repDict["Ancestor"],
                                                                          repDict["SourceSE"], repDict["TargetSE"] ) )
      transferSURLs = self._getTransferURLs( repDict, sourceSEs, {} )
      if not transferSURLs["OK"]:
        return transferSURLs
      sourceSURL, targetSURL, fileStatus = transferSURLs["Value"]
      # # TODO
      # # save ancestor to swap
      # if sourceSURL == targetSURL and waitingFileStatus.startswith( "Done" ):
      #  oldAncestor = str(channelID)
      #  newAncestor = waitingFileStatus[5:]
      #  ancestorSwap[ oldAncestor ] = newAncestor

    ftsFile = FTSFile()
    for key in ( "LFN", "FileID", "OperationID", "Checksum", "ChecksumType", "Size" ):
      setattr( ftsFile, key, fileJSON.get( key ) )

    # ftsFile.TargetSE = ",".join( targetSEs )
    ftsFile.Status = "Waiting"

    try:
      put = gFTSDB.putFTSFile( ftsFile )
      if not put["OK"]:
        gLogger.error( put["Message"] )
        return put
    except Exception, error:
      gLogger.exception( error )
      return S_ERROR( str( error ) )

  types_getFTSFile = [ LongType ]
  @staticmethod
  def export_getFTSFile( ftsFileID ):
    """ get FTSFile from FTSDB """
    try:
      getFile = gFTSDB.getFTSFile( ftsFileID )
    except Exception, error:
      gLogger.exception( error )
      return S_ERROR( error )

    if not getFile["OK"]:
      gLogger.error( "getFTSFile: %s" % getFile["Message"] )
      return getFile
    # # serilize
    if getFile["Value"]:
      getFile = getFile["Value"].toXML( True )
      if not getFile["OK"]:
        gLogger.error( getFile["Message"] )
    return getFile

  types_putFTSFile = [ StringTypes ]
  @classmethod
  def export_putFTSFile( cls, ftsFileXML ):
    """ put FTSFile into FTSDB """
    ftsFile = FTSFile.fromXML( ftsFileXML )
    if not ftsFile["OK"]:
      gLogger.error( ftsFile["Message"] )
      return ftsFile
    ftsFile = ftsFile["Value"]
    isValid = cls.ftsValidator().validate( ftsFile )
    if not isValid["OK"]:
      gLogger.error( isValid["Message"] )
      return isValid
    try:
      return gFTSDB.putFTSFile( ftsFile )
    except Exception, error:
      gLogger.exception( error )
      return S_ERROR( error )

  types_deleteFTSFile = [ LongType ]
  @classmethod
  def export_deleteFTSFile( cls, ftsFileID ):
    """ delete FTSFile record given FTSFileID """
    try:
      deleteFTSFile = gFTSDB.deleteFTSFile( ftsFileID )
      if not deleteFTSFile["OK"]:
        gLogger.error( deleteFTSFile["Message"] )
      return deleteFTSFile
    except Exception, error:
      gLogger.exception( error )
      return S_ERROR( error )

  types_putFTSJob = [ StringTypes ]
  @classmethod
  def export_putFTSJob( cls, ftsJobXML ):
    """ put FTSLfn into FTSDB """
    ftsJob = FTSJob.fromXML( ftsJobXML )
    if not ftsJob["OK"]:
      gLogger.error( ftsJob["Message"] )
      return ftsJob
    ftsJob = ftsJob["Value"]
    isValid = cls.ftsValidator().validate( ftsJob )
    if not isValid["OK"]:
      gLogger.error( isValid["Message"] )
      return isValid
    try:
      return gFTSDB.putFTSJob( ftsJob )
    except Exception, error:
      gLogger.exception( error )
      return S_ERROR( error )


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


  def _getSurlForLFN( self, targetSE, lfn ):
    """ Get the targetSURL for the storage and LFN supplied.

    :param self: self reference
    :param str targetSURL: target SURL
    :param str lfn: LFN
    """
    res = self.storageFactory().getStorages( targetSE, protocolList = ["SRM2"] )
    if not res["OK"]:
      errStr = "getSurlForLFN: Failed to create SRM2 storage for %s: %s" % ( targetSE, res["Message"] )
      gLogger.error( errStr )
      return S_ERROR( errStr )
    storageObjects = res["Value"]["StorageObjects"]
    for storageObject in storageObjects:
      res = storageObject.getCurrentURL( lfn )
      if res["OK"]:
        return res
    gLogger.error( "getSurlForLFN: Failed to get SRM compliant storage.", targetSE )
    return S_ERROR( "getSurlForLFN: Failed to get SRM compliant storage." )

  def _getSurlForPFN( self, sourceSE, pfn ):
    """Creates the targetSURL for the storage and PFN supplied.

    :param self: self reference
    :param str sourceSE: source storage element
    :param str pfn: physical file name
    """
    res = self.replicaManager().getPfnForProtocol( [pfn], sourceSE )
    if not res["OK"]:
      return res
    if pfn in res["Value"]["Failed"]:
      return S_ERROR( res["Value"]["Failed"][pfn] )
    return S_OK( res["Value"]["Successful"][pfn] )

  def _getTransferURLs( self, lfn, repDict, replicas, ancestorSwap = None ):
    """ prepare TURLs for given LFN and replication tree

    TODO: refactor!!!

    :param self: self reference
    :param str lfn: LFN
    :param dict repDict: replication dictionary
    :param dict replicas: LFN replicas
    """

    hopSourceSE = repDict["SourceSE"]
    hopDestSE = repDict["TargetSE"]
    hopAncestor = repDict["Ancestor"]

    if ancestorSwap and str( hopAncestor ) in ancestorSwap:
      self.log.debug( "getTransferURLs: swapping Ancestor %s with %s" % ( hopAncestor,
                                                                         ancestorSwap[str( hopAncestor )] ) )
      hopAncestor = ancestorSwap[ str( hopAncestor ) ]

    # # get targetSURL
    res = self._getSurlForLFN( hopDestSE, lfn )
    if not res["OK"]:
      errStr = res["Message"]
      self.log.error( errStr )
      return S_ERROR( errStr )
    targetSURL = res["Value"]

    # get the sourceSURL
    if hopAncestor:
      status = "Waiting%s" % ( hopAncestor )
      res = self._getSurlForLFN( hopSourceSE, lfn )
      if not res["OK"]:
        errStr = res["Message"]
        self.log.error( errStr )
        return S_ERROR( errStr )
      sourceSURL = res["Value"]
    else:
      status = "Waiting"
      res = self._getSurlForPFN( hopSourceSE, replicas[hopSourceSE] )
      if not res["OK"]:
        sourceSURL = replicas[hopSourceSE]
      else:
        sourceSURL = res["Value"]

    return S_OK( ( sourceSURL, targetSURL, status ) )

