########################################################################
# $HeadURL $
# File: PutAndRegister.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2013/03/25 07:43:24
########################################################################

""" :mod: PutAndRegister
    ====================

    .. module: PutAndRegister
    :synopsis: putAndRegister operation handler
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    PutAndRegister operation handler
"""

__RCSID__ = "$Id $"

# #
# @file PutAndRegister.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/03/25 07:43:34
# @brief Definition of PutAndRegister class.

# # imports
from DIRAC import S_OK, S_ERROR, gMonitor
from DIRAC.RequestManagementSystem.private.BaseOperation import BaseOperation
from DIRAC.RequestManagementSystem.Client.Operation import Operation
from DIRAC.RequestManagementSystem.Client.File import File

########################################################################
class PutAndRegister( BaseOperation ):
  """
  .. class:: PutAndRegister

  PutAndRegister operation handler
  """

  def __init__( self, operation = None ):
    """c'tor

    :param self: self reference
    :param Operation operation: Operation instance
    """
    # # base class ctor
    BaseOperation.__init__( self, operation )
    # # gMonitor stuff
    gMonitor.registerActivity( "PutAtt", "File put attempts",
                               self.__class__.__name__, "Files/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "PutFail", "Failed file puts",
                               self.__class__.__name__, "Files/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "PutOK", "Successful file puts",
                               self.__class__.__name__, "Files/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "RegisterOK", "Successful file registrations",
                               self.__class__.__name__, "Files/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "RegisterFail", "Failed file registrations",
                               self.__class__.__name__, "Files/min", gMonitor.OP_SUM )

  def __call__( self ):
    """ PutAndRegister operation processing """

    # # list of targetSEs
    targetSEs = list( set( [ targetSE.strip() for targetSE in self.operation.TargetSE.split( "," )
                             if targetSE.strip() ] ) )
    if len( targetSEs ) != 1:
      self.log.error( "wrong value for TargetSE list = %s, should contain only one target!" % targetSEs )
      self.operation.Error = "Wrong parameters: TargetSE should contain only one targetSE"
      for opFile in self.operation:

        opFile.Status = "Failed"
        opFile.Error = "Wrong parameters: TargetSE should contain only one targetSE"

        gMonitor.addMark( "PutAtt", 1 )
        gMonitor.addMark( "PutFail", 1 )

      return S_ERROR( "TargetSE should contain only one target, got %s" % targetSEs )

    targetSE = targetSEs[0]
    catalog = self.operation.Catalogue

    # # loop over files
    for opFile in self.operation:
      # # get LFN
      lfn = opFile.LFN

      self.log.info( "processing file %s" % lfn )
      if opFile.Status != "Waiting":
        self.log.info( "skipping file %s, status is %s" % ( lfn, opFile.Status ) )
        continue

      gMonitor.addMark( "PutAtt", 1 )

      pfn = opFile.PFN
      guid = opFile.GUID
      checksum = opFile.Checksum

      # # missing parameters
      if "" in ( lfn, pfn, guid, checksum ):
        self.log.error( "missing parameters: %s" % ( ", ".join( [ k for k, v in { "PFN" : pfn,
                                                                                  "GUID" : guid,
                                                                                  "Checksum" : checksum,
                                                                                  "LFN" : lfn  }.items()
                                                                  if v in ( "", None ) ] ) ) )
        opFile.Status = "Failed"
        opFile.Error = "Wrong parameters"
        gMonitor.addMark( "PutFail", 1 )
        continue

      # # call RM at least
      putAndRegister = self.replicaManager().putAndRegister( lfn,
                                                             pfn,
                                                             targetSE,
                                                             guid = guid,
                                                             checksum = checksum,
                                                             catalog = catalog )

      if not putAndRegister["OK"]:
        gMonitor.addMark( "PutFail", 1 )
        self.dataLoggingClient().addFileRecord( lfn, "PutFail", targetSE, "", "PutAndRegister" )
        self.log.error( "completely failed to put and register file: %s" % putAndRegister["Message"] )
        opFile.Error = putAndRegister["Message"]
        self.operation.Error = putAndRegister["Message"]
        continue

      putAndRegister = putAndRegister["Value"]

      if lfn in putAndRegister["Failed"]:
        gMonitor.addMark( "PutFail", 1 )
        self.dataLoggingClient().addFileRecord( lfn, "PutFail", targetSE, "", "PutAndRegister" )
        reason = putAndRegister["Failed"][lfn]
        self.log.error( "failed to put and register file %s at %s: %s" % ( lfn, targetSE, reason ) )
        opFile.Error = reason
        self.operation.Error = reason
        continue

      if lfn in putAndRegister["Successful"]:

        if "put" not in putAndRegister["Successful"][lfn]:

          gMonitor.addMark( "PutFail", 1 )
          self.dataLoggingClient().addFileRecord( lfn, "PutFail", targetSE, "", "PutAndRegister" )

          self.log.info( "failed to put %s to %s" % ( lfn, targetSE ) )

          opFile.Error = "put failed"
          self.operation.Error = "put failed"

        elif "register" not in putAndRegister["Successful"][lfn]:

          gMonitor.addMark( "PutOK", 1 )
          gMonitor.addMark( "RegisterFail", 1 )

          self.dataLoggingClient().addFileRecord( lfn, "Put", targetSE, "", "PutAndRegister" )
          self.dataLoggingClient().addFileRecord( lfn, "RegisterFail", targetSE, "", "PutAndRegister" )

          putTime = putAndRegister["Successful"][lfn]["put"]
          self.log.info( "successfully put %s to %s in %s seconds" % ( lfn, targetSE, putTime ) )

          opFile.Error = "failed to register %s at %s" % ( lfn, targetSE )
          opFile.Status = "Failed"

          self.log.info( opFile.Error )
          self.addRegisterReplica( opFile, targetSE )

        else:

          gMonitor.addMark( "PutOK", 1 )
          gMonitor.addMark( "RegisterOK", 1 )

          self.dataLoggingClient().addFileRecord( lfn, "Put", targetSE, "", "PutAndRegister" )
          self.dataLoggingClient().addFileRecord( lfn, "Register", targetSE, "", "PutAndRegister" )

          opFile.Status = "Done"
          for op in ( "put", "register" ):
            opTime = putAndRegister["Value"]["Successful"][lfn][op]
            self.log.info( "successfully %s %s to %s in %s seconds" % ( op, lfn, targetSE, opTime ) )

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
