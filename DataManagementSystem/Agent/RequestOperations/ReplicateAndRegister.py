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
import re
# # from DIRAC
from DIRAC import S_OK, S_ERROR, gMonitor
from DIRAC.RequestManagementSystem.private.BaseOperation import BaseOperation
from DIRAC.RequestManagementSystem.Client.Operation import Operation
from DIRAC.RequestManagementSystem.Client.File import File
from DIRAC.DataManagementSystem.Client.FTSClient import FTSClient

########################################################################
class ReplicateAndRegister( BaseOperation ):
  """
  .. class:: ReplicateAndRegister

  ReplicateAndRegister operation handler
  """
  __ftsClient = None

  def __init__( self, operation = None, csPath = None ):
    """c'tor

    :param self: self reference
    :param Operation operation: Operation instance
    :param str csPath: CS path for this handler
    """
    BaseOperation.__init__( self, operation, csPath )
    # # own gMonitor stuff for files
    gMonitor.registerActivity( "ReplicateAndRegisterAtt", "Replicate and register attempted",
                                "RequestExecutingAgent", "Files/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "ReplicateOK", "Replications successful",
                               "RequestExecutingAgent", "Files/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "ReplicateFail", "Replications failed",
                               "RequestExecutingAgent", "Files/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "RegisterOK", "Registrations successful",
                               "RequestExecutingAgent", "Files/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "RegisterFail", "Registrations failed",
                               "RequestExecutingAgent", "Files/min", gMonitor.OP_SUM )
    # # for FTS
    gMonitor.registerActivity( "FTSScheduleAtt", "Files schedule attempted",
                               "RequestExecutingAgent", "Files/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "FTSScheduleOK", "File schedule successful",
                               "RequestExecutingAgent", "Files/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "FTSScheduleFail", "File schedule failed",
                               "RequestExecutingAgent", "Files/min", gMonitor.OP_SUM )

  @classmethod
  def ftsClient( cls ):
    """ facade for FTS client """
    if not cls.__ftsClient:
      cls.__ftsClient = FTSClient()
    return cls.__ftsClient

  def __call__( self ):
    """ call me maybe """
    # #
    checkReplicas = self.__checkReplicas()
    if not checkReplicas["OK"]:
      self.log.error( checkReplicas["Message"] )
    if self.FTSMode:
      if self.OwnerGroup not in self.FTSBannedGroup:
        return self.ftsTransfer()
    return self.rmTransfer()

  def __checkReplicas( self ):
    """ check done replicas and update file states  """
    waitingFiles = dict( [ ( opFile.LFN, opFile ) for opFile in self.operation
                          if opFile.Status in ( "Waiting", "Scheduled" ) ] )
    targetSESet = set( self.operation.targetSEList )

    replicas = self.replicaManager().getCatalogReplicas( waitingFiles.keys() )
    if not replicas["OK"]:
      self.log.error( replicas["Message"] )
      return replicas

    reMissing = re.compile( "no such file or directory" )
    for failedLFN, errStr in replicas["Value"]["Failed"]:
      waitingFiles[failedLFN].Error = errStr
      if reMissing.search( errStr.lower() ):
        self.log.error( "file %s does not exists" % failedLFN )
        gMonitor.addMark( "ReplicateFail", len( targetSESet ) )
        waitingFiles[failedLFN].Status = "Failed"

    for successfulLFN, reps in replicas["Value"]["Successful"]:
      if targetSESet == set( reps ):
        self.log.info( "file %s has been replicated to all targets" % successfulLFN )
        waitingFiles[successfulLFN].Status = "Done"

    return S_OK()

  def ftsTransfer( self ):
    """ replicate and register using FTS """

    pass


  def rmTransfer( self ):
    """ replicate and register using ReplicaManager  """
    # # source SE
    sourceSE = self.operation.SourceSE
    # # check source se for read
    sourceRead = self.rssSEStatus( sourceSE, "Read" )
    if not sourceRead["OK"]:
      self.log.error( sourceRead["Message"] )
      for opFile in self.operation:
        opFile.Error = sourceRead["Message"]
        opFile.Status = "Failed"
      self.operation.Error = sourceRead["Message"]
      gMonitor.addMark( "ReplicateAndRegisterAtt", len( self.operation ) )
      gMonitor.addMark( "ReplicateFail", len( self.operation ) )
      return sourceRead

    if not sourceRead["Value"]:
      self.operation.Error = "SourceSE %s is banned for reading" % sourceSE
      self.log.error( self.operation.Error )
      return S_ERROR( self.operation.Error )

    # # list of targetSEs
    targetSEs = self.operation.targetSEList
    # # check targetSEs for removal
    bannedTargets = []
    for targetSE in targetSEs:
      writeStatus = self.rssSEStatus( targetSE, "Write" )
      if not writeStatus["OK"]:
        self.log.error( writeStatus["Message"] )
        for opFile in self.operation:
          opFile.Error = "unknown targetSE: %s" % targetSE
          opFile.Status = "Failed"
        self.operation.Error = "unknown targetSE: %s" % targetSE
        return S_ERROR( self.operation.Error )

      if not writeStatus["Value"]:
        self.log.error( "TargetSE %s in banned for writing right now" % targetSE )
        bannedTargets.append( targetSE )
        self.operation.Error += "banned targetSE: %s;" % targetSE
    # # some targets are banned? return
    if bannedTargets:
      return S_ERROR( "%s targets are banned for writing" % ",".join( bannedTargets ) )

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

      # # get waiting files
      waitingFiles = self.getWaitingFilesList()

      # # loop over files
      for opFile in waitingFiles:

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
    registerOperation.Type = "RegisterFile"
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
