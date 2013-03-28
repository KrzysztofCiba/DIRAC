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

  """

  def __init__( self, operation = None ):
    """c'tor

    :param self: self reference
    :param Operation operation: Operation instance
    """
    # # base class ctor
    BaseOperation.__init__( self, operation )
    # # gMonitor stuff
    gMonitor.registerActivity( "File put failed", "Failed puts",
                               "PutAndRegister", "Failed/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "File put successful", "Successful puts",
                               "PutAndRegister", "Successful/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "File registration successful", "Successful file registrations",
                               "PutAndRegister", "Successful/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "File registration failed", "Failed file registrations",
                               "PutAndRegister", "Failed/min", gMonitor.OP_SUM )

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
      return S_ERROR( "TargetSE should contain only one target, got %s" % targetSEs )

    targetSE = targetSEs[0]
    # # dict for failed LFNs
    failed = {}
    catalog = self.operation.Catalogue

    for opFile in self.operation:
      lfn = opFile.LFN

      self.log.info( "processing file %s" % lfn )
      if opFile.Status != "Waiting":
        self.log.info( "skipping file %s, status is %s" % ( lfn, opFile.Status ) )
        continue

      gMonitor.addMark( "Files Put", 1 )

      pfn = opFile.PFN if opFile.PFN else ""
      guid = opFile.GUID if opFile.GUID else ""
      checksum = opFile.Checksum if opFile.Checksum else ""

      # # missing parameters
      if "" in [ lfn, pfn, guid, checksum ]:
        self.log.error( "missing parameters: %s" % ( ", ".join( [ k for k, v in { "PFN" : pfn,
                                                                                  "GUID" : guid,
                                                                                  "Checksum" : checksum,
                                                                                  "LFN" : lfn  }.items()
                                                                  if v in ( "", None ) ] ) ) )


        self.log.error( "setting file status to 'Failed' and Error to 'Wrong Params'" )
        opFile.Status = "Failed"
        opFile.Error = "Wrong params"
        continue

      # # call RM at least
      putAndRegister = self.replicaManager().putAndRegister( lfn,
                                                             pfn,
                                                             targetSE,
                                                             guid = guid,
                                                             checksum = checksum,
                                                             catalog = catalog )

      if putAndRegister["OK"]:
        if lfn in putAndRegister["Value"]["Successful"]:

          if "put" not in putAndRegister["Value"]["Successful"][lfn]:

            gMonitor.addMark( "Put failed", 1 )
            self.dataLoggingClient().addFileRecord( lfn, "PutFail", targetSE, "", "TransferAgent" )
            self.log.info( "failed to put %s to %s." % ( lfn, targetSE ) )
            failed[lfn] = "put failed at %s" % targetSE
            self.operation.Error = "failed to put"

          elif "register" not in putAndRegister["Value"]["Successful"][lfn]:

            gMonitor.addMark( "Put successful", 1 )
            gMonitor.addMark( "File registration failed", 1 )

            self.dataLoggingClient().addFileRecord( lfn, "Put", targetSE, "", "TransferAgent" )
            self.dataLoggingClient().addFileRecord( lfn, "RegisterFail", targetSE, "", "TransferAgent" )

            putTime = putAndRegister["Value"]["Successful"][lfn]["put"]
            self.log.info( "successfully put %s to %s in %s seconds" % ( lfn, targetSE, putTime ) )
            self.log.info( "failed to register %s at %s" % ( lfn, targetSE ) )
            opFile.Error = "failed to register"
            opFile.Status = "Failed"
            self.log.info( "setting registration request for failed file" )

            registerOperation = Operation()
            registerOperation.TargetSE = targetSE
            registerOperation.Type = "RegisterFile"
            registerOperation.Catalogue = self.operation.Catalogue
            registerFile = File()
            registerFile.LFN = opFile.LFN
            registerFile.PFN = opFile.PFN
            registerFile.Size = opFile.Size
            registerFile.Checksum = opFile.Checksum
            registerFile.ChecksumType = opFile.ChecksumType
            registerFile.GUID = opFile.GUID
            registerOperation.addFile( registerFile )
            self.request.insertAfter( self.operation, registerOperation )

          else:

            self.addMark( "Put successful", 1 )
            self.addMark( "File registration successful", 1 )
            self.dataLoggingClient().addFileRecord( lfn, "Put", targetSE, "", "TransferAgent" )
            self.dataLoggingClient().addFileRecord( lfn, "Register", targetSE, "", "TransferAgent" )
            putTime = putAndRegister["Value"]["Successful"][lfn]["put"]
            self.log.info( "successfully put %s to %s in %s seconds" % ( lfn, targetSE, putTime ) )
            registerTime = putAndRegister["Value"]["Successful"][lfn]["register"]
            self.log.info( "successfully registered %s to %s in %s seconds" % ( lfn,
                                                                                targetSE,
                                                                                registerTime ) )

        else:

          self.addMark( "Put failed", 1 )
          self.dataLoggingClient().addFileRecord( lfn, "PutFail", targetSE, "", "PutAndRegister" )
          reason = putAndRegister["Value"]["Failed"][lfn]
          self.log.error( "failed to put and register file %s at %s: %s" % ( lfn,
                                                                         targetSE,
                                                                         reason ) )
          self.operation.Error = str( reason )[:255]
          failed[lfn] = reason

      else:

        self.addMark( "Put failed", 1 )
        self.dataLoggingClient().addFileRecord( lfn, "PutFail", targetSE, "", "TransferAgent" )
        self.log.error( "completely failed to put and register file: %s" % putAndRegister["Message"] )
        reason = putAndRegister["Message"]
        self.operation.Error = str( reason )[:255]
        failed[lfn] = reason

      if lfn not in failed:
        self.log.info( "file %s processed successfully, setting its status do 'Done'" % lfn )
        opFile.Status = "Done"
      else:
        self.log.error( "processing of file %s failed" % lfn )
        self.log.error( "reason: %s" % failed[lfn] )

    return S_OK()
