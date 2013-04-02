########################################################################
# $HeadURL $
# File: RemoveReplica.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2013/03/25 07:45:06
########################################################################

""" :mod: RemoveReplica
    =======================

    .. module: RemoveReplica
    :synopsis: removeReplica operation handler
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    removeReplica operation handler
"""

__RCSID__ = "$Id $"

# #
# @file RemoveReplica.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/03/25 07:45:17
# @brief Definition of RemoveReplica class.

# # imports
import os
# # from DIRAC
from DIRAC import S_OK, S_ERROR, gMonitor
from DIRAC.RequestManagementSystem.private.BaseOperation import BaseOperation
from DIRAC.FrameworkSystem.Client.ProxyManagerClient import gProxyManager
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getGroupsWithVOMSAttribute

########################################################################
class RemoveReplica( BaseOperation ):
  """
  .. class:: RemoveReplica

  """

  def __init__( self, operation ):
    """c'tor

    :param self: self reference
    """
    # # base class ctor
    BaseOperation.__init__( self, operation )
    # # gMonitor stuff
    gMonitor.registerActivity( "RemoveReplicaAtt", "Replica removals attempted",
                               "RemoveReplica", "Files/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "RemoveReplicaeOK", "Successful replica removals",
                               "RemoveReplica", "Files/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "RemoveReplicaFail", "Failed replica removals",
                               "RemoveReplica", "Files/min", gMonitor.OP_SUM )

  def __call__( self ):
    """ remove replicas """
    # # prepare list of targetSEs
    targetSEs = list( set( [ targetSE.strip() for targetSE in self.operation.TargetSE.split( "," )
                            if targetSE.strip() ] ) )
    toRemoveDict = dict( [ ( opFile.LFN, opFile ) for opFile in self.operation if opFile.Status == "Waiting" ] )

    self.log.info( "found %s replicas to delete from %s sites" % ( len( toRemoveDict ), len( targetSEs ) ) )
    gMonitor.addMark( "RemoveReplicaAtt", len( toRemoveDict ) * len( targetSEs ) )

    # # check targetSEs for removal
    bannedTargets = []
    for targetSE in targetSEs:
      removeStatus = self.rssSEStatus( targetSE, "Remove" )
      if not removeStatus["OK"]:
        self.log.error( removeStatus["Message"] )
        for opFile in self.operation:
          opFile.Error = "unknown targetSE: %s" % targetSE
          opFile.Status = "Failed"
        self.operation.Error = "unknown targetSE: %s" % targetSE
        return S_ERROR( self.operation.Error )

      removeStatus = removeStatus["Value"]
      if not removeStatus:
        self.log.error( "%s in banned for remove right now" % targetSE )
        bannedTargets.append( targetSE )
        self.operation.Error += "banned targetSE: %s;" % targetSE 
    # # some targets are banned? return
    if bannedTargets:
      return S_ERROR( "targets %s are banned for removal" % ",".join( bannedTargets ) )

    # # keep status for each targetSE
    removalStatus = dict.fromkeys( toRemoveDict.keys(), None )
    for lfn in removalStatus:
      removalStatus[lfn] = dict.fromkeys( targetSEs, None )

    for targetSE in targetSEs:

      self.log.info( "removing replicas at %s" % targetSE )

      # # 1st step - bulk removal
      bulkRemoval = self.bulkRemoval( toRemoveDict, targetSE )
      if not bulkRemoval["OK"]:
        self.log.error( bulkRemoval["Message"] )
        continue
      bulkRemoval = bulkRemoval["Value"]

      # # update removal status for successful files
      removalOK = [ opFile for opFile in bulkRemoval.values() if not opFile.Error ]
      for opFile in removalOK:
        removalStatus[lfn][targetSE] = ""
      gMonitor.addMark( "RemoveReplicaOK", len( removalOK ) )

      # # 2nd step - process the rest again
      toRetry = dict( [ ( lfn, opFile ) for lfn, opFile in bulkRemoval.items() if opFile.Error ] )
      for lfn, opFile in toRetry.items():
        self.singleRemoval( opFile, targetSE )
        if not opFile.Error:
          gMonitor.addMark( "RemoveReplicaOK", 1 )
          removalStatus[lfn][targetSE] = ""
        else:
          gMonitor.addMark( "RemoveReplicaFail", 1 )
          removalStatus[lfn][targetSE] = opFile.Error

    # # update file status for waiting files
    failed = 0
    for opFile in self.operation:
      if opFile.Status == "Waiting":
        errors = [ error for error in removalStatus[lfn].values() if error ]
        if errors:
          failed += 1
          opFile.Error = ",".join( errors )
          if "Write access not permitted for this credential" in opFile.Error:
            opFile.Status = "Failed"
            continue
        opFile.Status = "Done"

    if failed:
      self.operation.Error = "failed to remove %s replicas" % failed

    return S_OK()

  def bulkRemoval( self, toRemoveDict, targetSE ):
    """ remove replicas :toRemoveDict: at :targetSE:

    :param dict toRemoveDict: { lfn: opFile, ... }
    :param str targetSE: target SE name
    """
    removeReplicas = self.replicaManager().removeReplica( targetSE, toRemoveDict.keys() )
    if not removeReplicas["OK"]:
      for opFile in toRemoveDict.values():
        opFile.Error = removeReplicas["Message"]
      return S_ERROR( removeReplicas["Message"] )
    removeReplicas = removeReplicas["Value"]
    for lfn, opFile in toRemoveDict.items():
      if lfn in removeReplicas["Failed"]:
        opFile.Error = removeReplicas["Failed"][lfn]
      else:
        opFile.Error = ""
    return S_OK()

  def singleRemoval( self, opFile, targetSE ):
    """ remove opFile replica from targetSE

    :param File opFile: File instance
    :param str targetSE: target SE name
    """
    proxyFile = None
    if "Write access not permitted for this credential" in opFile.Error:
      # # not a DataManger? set status to failed and return
      if "DataManager" not in self.shifter:
        opFile.Status = "Failed"
      else:
        # #  you're a data manager - save current proxy and get a new one for LFN and retry
        saveProxy = os.environ["X509_USER_PROXY"]
        try:
          proxyFile = self.getProxyForLFN( opFile.LFN )
          if not proxyFile["OK"]:
            opFile.Error = proxyFile["Message"]
          else:
            proxyFile = proxyFile["Value"]
            removeReplica = self.replicaManager().removeReplica( targetSE, opFile.LFN )
            if not removeReplica["OK"]:
              opFile.Error = removeReplica["Message"]
            else:
              removeFile = removeReplica["Value"]
              if opFile.LFN in removeFile["Failed"]:
                opFile.Error = removeFile["Failed"][opFile.LFN]
              else:
                # # reset error - replica has been removed this time
                opFile.Error = ""
        finally:
          if proxyFile:
            os.unlink( proxyFile )
          # # put back request owner proxy to env
          os.environ["X509_USER_PROXY"] = saveProxy
    return S_OK( opFile )
