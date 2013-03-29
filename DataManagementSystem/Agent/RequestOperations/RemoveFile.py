########################################################################
# $HeadURL $
# File: RemoveFile.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2013/03/25 07:44:19
########################################################################

""" :mod: RemoveFile
    =======================

    .. module: RemoveFile
    :synopsis: removeFile operation handler
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    removeFile operation handler
"""

__RCSID__ = "$Id $"

# #
# @file RemoveFile.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/03/25 07:44:27
# @brief Definition of RemoveFile class.

# # imports
import os
from types import DictType
# # from DIRAC
from DIRAC import S_OK, gMonitor
from DIRAC.RequestManagementSystem.private.BaseOperation import BaseOperation
from DIRAC.FrameworkSystem.Client.ProxyManagerClient import gProxyManager

########################################################################
class RemoveFile( BaseOperation ):
  """
  .. class:: RemoveFile

  remove file operation handler
  """

  def __init__( self, operation ):
    """c'tor

    :param self: self reference
    """
    # # call base class ctor
    BaseOperation.__init__( self, operation )
    # # gMOnitor stuff goes here
    gMonitor.registerActivity( "RemoveFileAtt", "File removals attempted",
                               "RemoveFile", "Files/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "RemoveFileOK", "Successful file removals",
                               "RemoveFile", "Files/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "RemoveFileFail", "Failed file removals",
                               "RemoveFile", "Files/min", gMonitor.OP_SUM )

  def __call__( self ):
    """ action for 'removeFile' operation  """
    # #get waiting files
    toRemoveDict = dict( [ ( opFile.LFN, opFile ) for opFile in self.operation if opFile.Status == "Waiting" ] )
    gMonitor.addMark( "RemoveFileAtt", len( toRemoveDict ) )

    # # 1st step - bulk removal
    self.log.debug( "bulk removal of %s files" % len( toRemoveDict ) )
    bulkRemoval = self.bulkRemoval( toRemoveDict )
    if not bulkRemoval["OK"]:
      self.log.error( "bulk removal failed: %s" % bulkRemoval["Message"] )
    else:
      gMonitor.addMark( "RemovalFileOK", len( toRemoveDict ) - len( bulkRemoval["Value"] ) )
      toRemoveDict = bulkRemoval["Value"]

    # # 2nd step - single file removal
    for lfn, opFile in toRemoveDict.items():
      self.log.debug( "processing single file %s" % lfn )
      singleRemoval = self.singleRemoval( opFile )
      if not singleRemoval["OK"]:
        self.log.error( singleRemoval["Message"] )
        gMonitor.addMark( "RemoveFileFail", 1 )

      try:
        # # try to remove using proxy already defined in os.environ
        removal = self.replicaManager().removeFile( lfn )
        # # file is not existing?
        if not removal["OK"] and "no such file or directory" in str( removal["Message"] ).lower():
          removalStatus[lfn] = removal["Message"]
          continue
        # # not OK but request belongs to DataManager?
        if not self.requestOwnerDN and \
           ( not removal["OK"] and "Write access not permitted for this credential." in removal["Message"] ) or \
           ( removal["OK"] and "Failed" in removal["Value"] and
             lfn in removal["Value"]["Failed"] and
             "permission denied" in str( removal["Value"]["Failed"][lfn] ).lower() ):
          self.log.debug( "retrieving proxy for %s" % lfn )
          getProxyForLFN = self.getProxyForLFN( lfn )
          # # can't get correct proxy? continue...
          if not getProxyForLFN["OK"]:
            self.log.warn( "unable to get proxy for file %s: %s" % ( lfn, getProxyForLFN["Message"] ) )
            removal = getProxyForLFN
          else:
            # # you're a DataManager, retry with the new one proxy
            removal = self.replicaManager().removeFile( lfn )
      finally:
        # # make sure DataManager proxy is set back in place
        if not self.requestOwnerDN and self.dataManagerProxy():
          # # remove temp proxy
          if os.environ["X509_USER_PROXY"] != self.dataManagerProxy():
            os.unlink( os.environ["X509_USER_PROXY"] )
          # # put back DataManager proxy
          os.environ["X509_USER_PROXY"] = self.dataManagerProxy()

      # # save error
      if not removal["OK"]:
        removalStatus[lfn] = removal["Message"]
        continue
      # # check fail reason, filter out missing files
      removal = removal["Value"]
      if lfn in removal["Failed"]:
        removalStatus[lfn] = removal["Failed"][lfn]

    # # counters
    filesRemoved = 0
    filesFailed = 0
    subRequestError = []
    # # update File statuses and errors
    for lfn, error in removalStatus.items():

      # # set file error if any
      if error:
        self.log.debug( "%s: %s" % ( lfn, str( error ) ) )
        fileError = str( error ).replace( "'", "" )[:255]
        fileError = requestObj.setSubRequestFileAttributeValue( index, "removal", lfn,
                                                                "Error", fileError )
      # # no error? file not exists? - we are able to recover
      if not error or "no such file or directory" in str( error ).lower() or \
            "file does not exist in the catalog" in str( error ).lower():
        filesRemoved += 1
        self.log.info( "successfully removed %s" % lfn )
        updateStatus = requestObj.setSubRequestFileAttributeValue( index, "removal", lfn, "Status", "Done" )
        if not updateStatus["OK"]:
          self.log.error( "unable to change status to 'Done' for %s" % lfn )
      else:
        filesFailed += 1
        self.log.warn( "unable to remove file %s : %s" % ( lfn, error ) )
        errorStr = str( error )
        if type( error ) == DictType:
          errorStr = ";".join( [ "%s:%s" % ( key, value ) for key, value in error.items() ] )
        errorStr = errorStr.replace( "'", "" )
        subRequestError.append( "%s:%s" % ( lfn, errorStr ) )

    self.addMark( "RemoveFileSucc", filesRemoved )
    self.addMark( "RemoveFileFail", filesFailed )

    if filesFailed:
      self.log.info( "all files processed, %s files failed to remove" % filesFailed )
      self.operation.Error = ";".join( subRequestError )[:255]

    return S_OK()

  def bulkRemoval( self, toRemoveDict ):
    """ bulk removal using request owner DN

    :param dict toRemoveDict: { lfn: opFile, ... }
    :return: S_ERROR or S_OK( { lfn: opFile, ... } ) -- dict with files still waiting to be removed
    """
    bulkRemoval = self.replicaManager().removeFile( toRemoveDict.keys() )
    if not bulkRemoval["OK"]:
      self.log.error( "unable to remove files: %s" % bulkRemoval["Message"] )
      self.operation.Error = bulkRemoval["Message"]
      return bulkRemoval
    bulkRemoval = bulkRemoval["Value"]
    # # filter results
    for lfn, opFile in toRemoveDict.items():
      if lfn in bulkRemoval["Successful"]:
        opFile.Status = "Done"
      elif lfn in bulkRemoval["Failed"]:
        opFile.Error = bulkRemoval["Failed"][lfn]
        if "no such file or directory" in str( opFile.Error ).lower():
          removeFromCatalog = self.replicaManager().removeCatalogFile( lfn, singleFile = True )
          if removeFromCatalog["OK"]:
            opFile.Status = "Done"
            continue
    # # return files still waiting
    toRemoveDict = dict( [ ( opFile.LFN, opFile ) for opFile in self.operation if opFile.Status == "Waiting" ] )
    return S_OK( toRemoveDict )

  def singleRemoval( self, opFile ):
    """ remove single file """

    pass

def withProxyForLFN( self, lfn ):
    """ get proxy for LFN

    :param self: self reference
    :param str lfn: LFN
    """

    dirMeta = self.replicaManager().getCatalogDirectoryMetadata( lfn, singleFile = True )
    if not dirMeta["OK"]:
      return dirMeta
    dirMeta = dirMeta["Value"]

    ownerRole = "/%s" % dirMeta["OwnerRole"] if not dirMeta["OwnerRole"].startswith( "/" ) else dirMeta["OwnerRole"]
    ownerDN = dirMeta["OwnerDN"]

    ownerProxy = None
    for ownerGroup in getGroupsWithVOMSAttribute( ownerRole ):
      vomsProxy = gProxyManager.downloadVOMSProxy( ownerDN, ownerGroup, limited = True,
                                                   requiredVOMSAttribute = ownerRole )
      if not vomsProxy["OK"]:
        self.debug( "getProxyForLFN: failed to get VOMS proxy for %s role=%s: %s" % ( ownerDN,
                                                                                      ownerRole,
                                                                                      vomsProxy["Message"] ) )
        continue
      ownerProxy = vomsProxy["Value"]
      self.debug( "getProxyForLFN: got proxy for %s@%s [%s]" % ( ownerDN, ownerGroup, ownerRole ) )
      break

    if not ownerProxy:
      return S_ERROR( "Unable to get owner proxy" )

    dumpToFile = ownerProxy.dumpAllToFile()
    if not dumpToFile["OK"]:
      self.error( "getProxyForLFN: error dumping proxy to file: %s" % dumpToFile["Message"] )
      return dumpToFile
    dumpToFile = dumpToFile["Value"]
    os.environ["X509_USER_PROXY"] = dumpToFile

    return S_OK()
