########################################################################
# $HeadURL $
# File: ReplicateAndRegisterOperation.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2013/03/13 18:49:12
########################################################################
""" :mod: ReplicateAndRegisterOperation
    =======================

    .. module: ReplicateAndRegisterOperation
    :synopsis: ReplicateAndRegisterOperation operation handler
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    ReplicateAndRegisterOperation operation handler
"""
__RCSID__ = "$Id $"
# #
# @file ReplicateAndRegisterOperation.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/03/13 18:49:28
# @brief Definition of ReplicateAndRegisterOperation class.

# # imports
from DIRAC import S_OK, S_ERROR, gMonitor
from DIRAC.RequestManagementSystem.private.BaseOperation import BaseOperation

########################################################################
class ReplicateAndRegisterOperation( BaseOperation ):
  """
  .. class:: ReplicateAndRegisterOperation

  ReplicateAndRegisterOperation operation handler
  """

  def __init__( self, operation ):
    """c'tor

    :param self: self reference
    """
    BaseOperation.__init__( self, operation )
    # # own gMonitor stuff for files

  def __call__( self ):
    """ call me maybe """
    # # list of targetSEs
    targetSEs = list( set( [ targetSE.strip() for targetSE in self.operation.TargetSE.split( "," )
                             if targetSE.strip() ] ) )
    # # source SE
    sourceSE = self.operation.SourceSE if self.operation.SourceSE else ""

    # # dict for failed lfns
    failed = {}

    for targetSE in targetSEs:
      for opFile in self.operation:
        if opFile.Status != "Waiting":
          continue

        lfn = opFile.LFN
        failed.setdefault( lfn, { } )

        gMonitor.addMark( "Replicate and register", 1 )
        res = self.replicaManager().replicateAndRegister( lfn, targetSE, sourceSE = sourceSE )

        if res["OK"]:
          if lfn in res["Value"]["Successful"]:
            if "replicate" in res["Value"]["Successful"][lfn]:
              repTime = res["Value"]["Successful"][lfn]["replicate"]
              self.log.info( "file %s replicated at %s in %s s." % ( lfn, targetSE, repTime ) )
              gMonitor.addMark( "Replication successful", 1 )
              if "register" in res["Value"]["Successful"][lfn]:
                gMonitor.addMark( "Replica registration successful", 1 )
                regTime = res["Value"]["Successful"][lfn]["register"]
                self.log.info( "file %s registered at %s in %s s." % ( lfn, targetSE, regTime ) )
              else:
                gMonitor.addMark( "Replica registration failed", 1 )
                self.log.info( "failed to register %s at %s." % ( lfn, targetSE ) )
                opFile.Error = "Failed to register"

                # # TODO: add RegisterFile operation here


                # fileDict = res["Value"]["Failed"][lfn]["register"]
                # registerRequestDict = {
                #  "Attributes" : {
                #    "TargetSE" : fileDict["TargetSE"],
                #    "Operation": "registerReplica" },
                #  "Files": [ {
                #      "LFN" : fileDict["LFN"],
                #      "PFN" : fileDict["PFN"] } ] }
                # self.info( "replicateAndRegister: adding registration request for failed replica." )
                # requestObj.addSubRequest( registerRequestDict, "register" )

            else:
              self.log.info( "failed to replicate %s to %s." % ( lfn, targetSE ) )
              gMonitor.addMark( "Replication failed", 1 )
              opFile.Error = "Failed to replicate"
              failed[lfn][targetSE] = "Replication failed for %s at %s" % ( lfn, targetSE )
          else:
            gMonitor.addMark( "Replication failed", 1 )
            reason = res["Value"]["Failed"][lfn]
            self.log.error( "failed to replicate and register file %s at %s: %s" % ( lfn, targetSE, reason ) )
            failed[lfn][targetSE] = reason
        else:
          gMonitor.addMark( "Replication failed", 1 )
          opFile.Error = "ReplicaManager error: %s" % res["Message"]
          self.log.error( opFile.Error )
          failed[lfn][targetSE] = res["Message"]

      if not failed[lfn]:
        self.log.info( "file %s has been successfully processed at all targetSEs" % lfn )
        opFile.Status = "Done"
      else:
        self.log.error( "replication of %s failed: %s" % ( lfn, failed[lfn] ) )
        opFile.Status = "Failed"

    return S_OK()

