########################################################################
# $HeadURL $
# File: RegisterOperation.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2013/03/19 13:55:14
########################################################################

""" :mod: RegisterOperation 
    =======================
 
    .. module: RegisterOperation
    :synopsis: register operation handler
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    register operation handler
"""

__RCSID__ = "$Id $"

##
# @file RegisterOperation.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/03/19 13:55:24
# @brief Definition of RegisterOperation class.

## imports 
from DIRAC import gMonitor, S_OK, S_ERROR
from DIRAC.RequestManagementSystem.private.BaseOperation import BaseOperation

########################################################################
class RegisterFile( BaseOperation ):
  """
  .. class:: RegisterOperation
  
  register operation handler
  """

  def __init__( self, operation ):
    """c'tor

    :param self: self reference
    """
    BaseOperation.__init__( self, operation )
    ## register specific monitor info
    gMonitor.registerActivity( "RegFileSucc", "Registration successful",
                               self.__class__.__name__, "Files/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "RegFileFail", "Registration failed",
                               self.__class__.__name__, "Files/min", gMonitor.OP_SUM )
    
  def __call__( self ):
    """ call me maybe """
    ## list of targetSE
    targetSEs = list( set( [ targetSE.strip() for targetSE in self.operation.TargetSE.split(",") 
                             if targetSE.strip() ] ) )
    if not targetSEs:
      self.error( "targetSE missing" )
      self.operation.Error = "TargetSE is not specified"
      self.operation.Status = "Failed"
      return S_ERROR( self.operation.Error )

    ## dict for failed LFNs
    failed = {}
    failedFiles = 0

    # # get cataloge
    catalogue = self.operation.Catalogue

    for opFile in self.operation:
      if opFile.Status != "Waiting":
        continue

      lfn = opFile.LFN
      failed.setdefault( lfn, {} )
      pfn = opFile.PFN
      size = opFile.Size 
      guid = opFile.GUID
      chksum = opFile.Checksum

      for targetSE in targetSEs:
        
        fileTuple = ( lfn, pfn, size, targetSE, guid, chksum )
        res = self.replicaManager().registerFile( fileTuple, catalogue )
        
        if not res["OK"] or lfn in res["Value"]["Failed"]:
          self.dataLoggingClient().addFileRecord( lfn, "RegisterFail", targetSE, "", "RegisterOperation" )
          reason = res["Message"] if not res["OK"] else res["Value"]["Failed"][lfn] # "registration in ReplicaManager failed"
          errorStr = "failed to register LFN %s: %s" % ( lfn, reason )
          failed[lfn][targetSE] = reason
          self.log.warn( errorStr )
          failedFiles += 1
        else:
          self.dataLoggingClient().addFileRecord( lfn, "Register", targetSE, "", "RegisterOperation" )
          self.info( "file %s has been registered at %s" % ( lfn, targetSE ) )
     
      if not failed[lfn]:
        opFile.Status = "Done"
        self.info( "registerFile: file %s has been registered at all targetSEs" % lfn )        
      else:
        opFile.Error =  ";".join( [ "%s:%s" % (targetSE, reason.replace("'", "") ) 
                                    for targetSE, reason in failed[lfn].items() ] )

    if failedFiles:
      self.log.info("all files processed, %s files failed to register" % failedFiles )
      errors = []
      for lfn in failed:
        for targetSE, reason in failed[lfn].items():
          error = "%s:%s:%s" % ( lfn, targetSE, reason.replace("'", "") )
          self.log.warn( "registerFile: %s@%s - %s" % ( lfn, targetSE, reason ) )
          errors.append( error )
      self.operation.Error = "Some files failed to register"
      return S_ERROR( self.operation.Error )

    self.operation.Status = "Done"
    return S_OK()



