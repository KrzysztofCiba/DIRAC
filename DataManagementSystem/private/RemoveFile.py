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

##
# @file RemoveFile.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/03/25 07:44:27
# @brief Definition of RemoveFile class.

## imports 
import os
# # from DIRAC
from DIRAC import S_OK, gMonitor
from DIRAC.RequestManagementSystem.private.BaseOperation import BaseOperation
from types import DictType

########################################################################
class RemoveFile(BaseOperation):
  """
  .. class:: RemoveFile
  
  """

  def __init__( self, operation ):
    """c'tor

    :param self: self reference
    """
    ## call base class ctor
    BaseOperation.__init__(self, operation)

  def __call__( self ):
    """ action for 'removeFile' operation  """

    lfns = [ opFile.LFN for opFile in self.operation if opFile.Status == "Waiting" ]
    self.log.debug( "about to remove %d files" % len( lfns ) )
    # # keep removal status for each file
    removalStatus = dict.fromkeys( lfns, "" )
    gMonitor.addMark( "RemoveFileAtt", len( lfns ) )

    # # bulk removal 1st
    bulkRemoval = self.replicaManager().removeFile( lfns )
    if not bulkRemoval["OK"]:
      self.log.error( "unable to remove files: %s" % bulkRemoval["Message"] )
      self.operation.Error = bulkRemoval["Message"][:255]
      return bulkRemoval

    bulkRemoval = bulkRemoval["Value"]
    failedLfns = bulkRemoval["Failed"] if "Failed" in bulkRemoval else []
    toRemove = []
    for lfn in removalStatus:
      if lfn in failedLfns and "no such file or directory" in str( bulkRemoval["Failed"][lfn] ).lower():
        removalStatus[lfn] = bulkRemoval["Failed"][lfn]
        removeCatalog = self.replicaManager().removeCatalogFile( lfn, singleFile = True )
        if not removeCatalog["OK"]:
          removalStatus[lfn] = removeCatalog["Message"]
          continue
      else:
        toRemove.append( lfn )

    # # loop over LFNs to remove
    for lfn in toRemove:
      self.log.debug( "processing file %s" % lfn )
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

