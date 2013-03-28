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

# #
# @file RegisterOperation.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/03/19 13:55:24
# @brief Definition of RegisterOperation class.

# # imports
from DIRAC import gMonitor, S_OK, S_ERROR
from DIRAC.RequestManagementSystem.private.BaseOperation import BaseOperation

########################################################################
class RegisterFile( BaseOperation ):
  """
  .. class:: RegisterOperation

  register operation handler
  """

  def __init__( self, operation = None ):
    """c'tor

    :param self: self reference
    :param Operation operation: Operation instance
    """
    BaseOperation.__init__( self, operation )
    # # RegisterFile specific monitor info
    gMonitor.registerActivity( "RegFileOK", "Successful file registrations",
                               self.__class__.__name__, "Files/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "RegFileFail", "Failed file registrations",
                               self.__class__.__name__, "Files/min", gMonitor.OP_SUM )

  def __call__( self ):
    """ call me maybe """
    # # list of targetSE
    targetSEs = list( set( [ targetSE.strip() for targetSE in self.operation.TargetSE.split( "," )
                             if targetSE.strip() ] ) )
    if len( targetSEs ) != 1:
      self.log.error( "wrong TargetSE attribute, expecting one entry, got %s" % len( targetSEs ) )
      self.operation.Error = "Wrongly formatted TargetSE"
      self.operation.Status = "Failed"
      return S_ERROR( self.operation.Error )
    targetSE = targetSEs[0]
    # # counter for failed files
    failedFiles = 0
    # # catalogue to use
    catalogue = self.operation.Catalogue
    # # loop over files
    for opFile in self.operation:
      # # skip non-waiting
      if opFile.Status != "Waiting":
        continue
      # # get LFN
      lfn = opFile.LFN
      # # and others
      fileTuple = ( lfn , opFile.PFN, opFile.Size, targetSE, opFile.GUID, opFile.Checksum )
      # # call ReplicaManager
      registerFile = self.replicaManager().registerFile( fileTuple, catalogue )
      # # check results
      if not registerFile["OK"] or lfn in registerFile["Value"]["Failed"]:
        gMonitor.addMark( "RegFileFail", 1 )
        self.dataLoggingClient().addFileRecord( lfn, "RegisterFail", targetSE, "", "RegisterFile" )
        reason = registerFile["Message"] if not registerFile["OK"] else registerFile["Value"]["Failed"][lfn]
        errorStr = "failed to register LFN %s: %s" % ( lfn, reason )
        opFile.Error = reason
        self.log.warn( errorStr )
        failedFiles += 1
      else:
        gMonitor.addMark( "RegFileOK", 1 )
        self.dataLoggingClient().addFileRecord( lfn, "Register", targetSE, "", "RegisterFile" )
        self.info( "file %s has been registered at %s" % ( lfn, targetSE ) )
        opFile.Status = "Done"
    # # final check
    if failedFiles:
      self.log.info( "all files processed, %s files failed to register" % failedFiles )
      self.operation.Error = "some files failed to register"
      return S_ERROR( self.operation.Error )

    return S_OK()



