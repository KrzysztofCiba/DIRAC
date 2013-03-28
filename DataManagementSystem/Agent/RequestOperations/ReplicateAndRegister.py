########################################################################
# $HeadURL $
# File: ReplicateAndRegister.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2013/03/13 18:49:12
########################################################################
""" :mod: ReplicateAndRegister
    ==========================

    .. module: ReplicateAndRegister
    :synopsis: ReplicateAndRegister operation handler
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    ReplicateAndRegister operation handler
"""
__RCSID__ = "$Id $"
# #
# @file ReplicateAndRegister.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/03/13 18:49:28
# @brief Definition of ReplicateAndRegister class.

# # imports
from DIRAC import S_OK, S_ERROR, gMonitor
from DIRAC.RequestManagementSystem.private.BaseOperation import BaseOperation
from DIRAC.RequestManagementSystem.Client.Operation import Operation
from DIRAC.RequestManagementSystem.Client.File import File

########################################################################
class ReplicateAndRegister( BaseOperation ):
  """
  .. class:: ReplicateAndRegister

  ReplicateAndRegister operation handler
  """

  def __init__( self, operation = None ):
    """c'tor

    :param self: self reference
    :param Operation operation: Operation instance
    """
    BaseOperation.__init__( self, operation )
    # # own gMonitor stuff for files
    name = self.__class__.__name__
    gMonitor.registerActivity( "ReplicateAndRegisterAtt", "Replicate and register attempted",
                                name, "Files/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "ReplicateOK", "Replications successful",
                                name, "Files/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "ReplicateFail", "Replications failed",
                                name, "Files/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "RegisterOK", "Registrations successful",
                                name, "Files/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "RegisterFail", "Registrations failed",
                                name, "Files/min", gMonitor.OP_SUM )


  def __call__( self ):
    """ call me maybe """

    # # list of targetSEs
    targetSEs = list( set( [ targetSE.strip() for targetSE in self.operation.TargetSE.split( "," )
                             if targetSE.strip() ] ) )
    # # source SE
    sourceSE = self.operation.SourceSE

    sourceRead = self.rssSEStatus( sourceSE, "Read" )
    if not sourceRead["OK"]:
      self.log.error( sourceRead["Message"] )
      for opFile in self.operation:
        opFile.Error = sourceRead["Message"]
        opFile.Status = "Failed"
      self.operation.Error = sourceRead["Message"]
      return sourceRead

    if not sourceRead["Value"]:
      reason = "SourceSE %s is banned for reading" % sourceSE
      self.log.error( reason )
      self.operation.Error = reason
      return S_ERROR( reason )

    # # loop over targetSE
    for targetSE in targetSEs:

      # # check target SE
      targetWrite = self.rssSEStatus( targetSE, "Write" )
      if not targetWrite["OK"]:
        self.log.error( targetWrite["Message"] )
        for opFile in self.operation:
          opFile.Error = targetWrite["Message"]
          opFile.Status = "Failed"
        self.operation.Error = sourceRead["Message"]
        return targetWrite
      if not targetWrite["Value"]:
        reason = "TargetSE %s is banned for writing" % targetSE
        self.log.error( reason )
        self.operation.Error = reason
        continue

      # # loop over files
      for opFile in self.operation:
        # # skip non-waiting files
        if opFile.Status != "Waiting":
          continue

        gMonitor.addMark( "ReplicateAndRegisterAtt", 1 )
        lfn = opFile.LFN

        # # call ReplicaManager
        res = self.replicaManager().replicateAndRegister( lfn, targetSE, sourceSE = sourceSE )

        if res["OK"]:

          if lfn in res["Value"]["Successful"]:

            if "replicate" in res["Value"]["Successful"][lfn]:

              repTime = res["Value"]["Successful"][lfn]["replicate"]
              self.log.info( "file %s replicated at %s in %s s." % ( lfn, targetSE, repTime ) )

              gMonitor.addMark( "ReplicateOK", 1 )

              if "register" in res["Value"]["Successful"][lfn]:

                gMonitor.addMark( "RegisterOK", 1 )
                regTime = res["Value"]["Successful"][lfn]["register"]
                self.log.info( "file %s registered at %s in %s s." % ( lfn, targetSE, regTime ) )

              else:

                gMonitor.addMark( "RegisterFail", 1 )
                self.log.info( "failed to register %s at %s." % ( lfn, targetSE ) )

                opFile.Error = "Failed to register"
                opFile.Status = "Failed"
                # # add register replica operation
                self.addRegisterReplica( opFile, targetSE )

            else:

              self.log.info( "failed to replicate %s to %s." % ( lfn, targetSE ) )
              gMonitor.addMark( "ReplicateFail", 1 )
              opFile.Error = "Failed to replicate"

          else:

            gMonitor.addMark( "ReplicateFail", 1 )
            reason = res["Value"]["Failed"][lfn]
            self.log.error( "failed to replicate and register file %s at %s: %s" % ( lfn, targetSE, reason ) )
            opFile.Error = reason

        else:

          gMonitor.addMark( "ReplicateFail", 1 )
          opFile.Error = "ReplicaManager error: %s" % res["Message"]
          self.log.error( opFile.Error )

        if not opFile.Error:
          self.log.info( "file %s has been replicated to all targetSEs" % lfn )
          opFile.Status = "Done"

    return S_OK()

  def addRegisterReplica( self, opFile, targetSE ):
    """ add RegisterReplica operation for file

    :param File opFile: operation file
    :param str targetSE: target SE
    """
    # # add RegisterReplica operation
    registerOperation = Operation()
    registerOperation.Type = "RegisterReplica"
    registerOperation.TargetSE = targetSE

    registerFile = File()
    registerFile.LFN = opFile.LFN
    registerFile.PFN = opFile.PFN
    registerFile.GUID = opFile.GUID
    registerFile.Checksum = opFile.Checksum
    registerFile.ChecksumType = opFile.ChecksumType
    registerFile.Size = opFile.Size

    registerOperation.addFile( registerFile )
    self.request.insertAfter( registerOperation, self.operation )
    return S_OK()
