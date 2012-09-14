########################################################################
# $HeadURL $
# File: SubRequestTests.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2012/08/14 14:30:20
########################################################################

""" :mod: SubRequestTests 
    =======================
 
    .. module: SubRequestTests
    :synopsis: SubRequest test cases
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    SubRequest test cases
"""

__RCSID__ = "$Id $"

##
# @file SubRequestTests.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2012/08/14 14:30:34
# @brief Definition of SubRequestTests class.

## imports 
import unittest
## from DIRAC
from DIRAC.RequestManagementSystem.Client.SubReqFile import SubReqFile
## SUT
from DIRAC.RequestManagementSystem.Client.SubRequest import SubRequest

########################################################################
class SubRequestTests(unittest.TestCase):
  """
  .. class:: SubRequestTests
  
  """

  def setUp( self ):
    """ test set up """
    self.fromDict = { "RequestType" : "transfer",
                      "Operation" : "replicateAndRegister",
                      "TargetSE" : "CERN-USER,PIC-USER",
                      "SourceSE" : "" }
    self.subFile = SubReqFile( { "LFN" : "/lhcb/user/c/cibak/testFile",
                                 "Addler" : "1234567",
                                 "Size" : 1024,
                                 "Status" : "Waiting" } )
    self.subReq = None

  def tearDown( self ):
    """ test case tear down """
    del self.fromDict
    del self.subFile
 
  def test_ctor( self ):
    """ test constructors and (de)serialisation """
    ## empty ctor
    self.assertEqual( isinstance( SubRequest(), SubRequest), True )

    subReq = SubRequest()
  
    ## using fromDict
    subReq = SubRequest( self.fromDict )
    self.assertEqual( isinstance( subReq, SubRequest), True )
    for key, value in self.fromDict.items():
      self.assertEqual( getattr( subReq, key), value )
    ## from XML
    subReq = SubRequest.fromXML( subReq.toXML() )
    self.assertEqual( isinstance( subReq, SubRequest), True )
    for key, value in self.fromDict.items():
      self.assertEqual( getattr( subReq, key), value )
    ## same with file
    subReq = SubRequest( self.fromDict )
    subReq += self.subFile
    subReq = SubRequest.fromXML( subReq.toXML() )
    self.assertEqual( isinstance( subReq, SubRequest), True )
    for key, value in self.fromDict.items():
      self.assertEqual( getattr( subReq, key ), value )

    ## toSQL no SubRequestID
    self.assertEqual( subReq.toSQL().startswith("INSERT"), True )
    ## toSQL SubRequestID set
    subReq.SubRequestID = 10
    self.assertEqual( subReq.toSQL().startswith("UPDATE"), True )


  def test_props( self ):
    """ test properties """
    ## valid values
    subReq = SubRequest()
    subReq.SubRequestID = 1
    self.assertEqual( subReq.SubRequestID, 1 )
    subReq.SubRequestID = "1"
    self.assertEqual( subReq.SubRequestID, 1 )
    subReq.RequestID = 1
    self.assertEqual( subReq.RequestID, 1 )
    subReq.RequestID = "1"
    self.assertEqual( subReq.RequestID, 1 )

    operationDict = { "diset" : ( "commitRegisters", "setFileStatusForTransformation", "setJobStatusBulk",
                                  "sendXMLBookkeepingReport", "setJobParameters" ),
                      "logupload" : ( "uploadLogFiles", ),
                      "register" : ( "registeFile", "reTransfer" ),
                      "removal" : ( "replicaRemoval", "removeFile", "physicalRemoval" ),
                      "transfer" : ( "replicateAndRegister", "putAndRegister" ) }     
    for reqType, operations in operationDict.items():
      subReq = SubRequest()
      subReq.RequestType = reqType
      self.assertEqual( subReq.RequestType, reqType )
      for operation in operations:
        subReq.Operation = operation
        self.assertEqual( subReq.Operation, operation )

    subReq.Argument = "foobar"
    self.assertEqual( subReq.Argument, "foobar" )

    subReq.SourceSE = "CERN-RAW"
    self.assertEqual( subReq.SourceSE, "CERN-RAW" )

    subReq.TargetSE = "CERN-RAW"
    self.assertEqual( subReq.TargetSE, "CERN-RAW" )

    subReq.Catalogue = ""
    self.assertEqual( subReq.Catalogue, "" )

    subReq.Catalogue = "Bookkeeping"
    self.assertEqual( subReq.Catalogue, "Bookkeeping" )

    subReq.Error = "error"
    self.assertEqual( subReq.Error, "error" )

    subReq.ExecutionOrder = 1
    self.assertEqual( subReq.ExecutionOrder, 1 )


    ## wrong props
    try:
      subReq.RequestID = "foo"
    except Exception, error:
      self.assertEqual( type(error), ValueError )

    try:
      subReq.SubRequestID = "foo"
    except Exception, error:
      self.assertEqual( type(error), ValueError )
    
    ## Request type and Operation
    subReq = SubRequest()
    try:
      subReq.RequestType = 1
    except Exception, error:
      self.assertEqual( type(error), ValueError )
      self.assertEqual( str(error), "1 is not a valid request type!" )

    subReq = SubRequest()
    try:
      subReq.RequestType = "foo"
    except Exception, error:
      self.assertEqual( type(error), ValueError )
      self.assertEqual( str(error), "foo is not a valid request type!" )

    subReq = SubRequest()
    try:
      subReq.Operation = "foo"
    except Exception, error:
      self.assertEqual( type(error), ValueError )
      self.assertEqual( str(error), "'foo' in not valid Operation!")
    
    ## mismatch of Operation 
    subReq = SubRequest()
    subReq.RequestType = "transfer"
    try:
      subReq.Operation = "removeFile"
    except Exception, error:
      self.assertEqual( type(error), ValueError )
      self.assertEqual( str(error), "Operation 'removeFile' is not valid for 'transfer' request type!")

    ## mismacth of RequestType
    subReq = SubRequest()
    subReq.Operation = "removeFile"
    try:
      subReq.RequestType = "transfer"
    except Exception, error:
      self.assertEqual( type(error), ValueError )
      self.assertEqual( str(error), "RequestType 'transfer' is not valid for Operation 'removeFile'")
    
    ## timestamps
    subReq = SubRequest()
    try:
      subReq.CreationTime = "foo"
    except Exception, error:
      self.assertEqual( type(error), ValueError )
      self.assertEqual( str(error), "time data 'foo' does not match format '%Y-%m-%d %H:%M:%S'" )

    try:
      subReq.SubmissionTime = "foo"
    except Exception, error:
      self.assertEqual( type(error), ValueError )
      self.assertEqual( str(error), "time data 'foo' does not match format '%Y-%m-%d %H:%M:%S'" )

    try:
      subReq.LastUpdate = "foo"
    except Exception, error:
      self.assertEqual( type(error), ValueError )
      self.assertEqual( str(error), "time data 'foo' does not match format '%Y-%m-%d %H:%M:%S'" )
      
    ## Status
    subReq = SubRequest()
    try:
      subReq.Status = "foo"
    except Exception, error:
      self.assertEqual( type(error), ValueError )
      self.assertEqual( str(error), "unknown Status 'foo'" )

    subReq += SubReqFile( { "Status" : "Waiting" } )
    oldStatus = subReq.Status 
    ## won't modify - there are Waiting files
    subReq.Status = "Done"
    self.assertEqual( subReq.Status, oldStatus )
    ## won't modify - there are Scheduled files 
    for subFile in subReq:
      subFile.Status = "Scheduled"
    subReq.Status = "Done"
    self.assertEqual( subReq.Status, oldStatus )
    ## will modify - all fileas are Done now
    for subFile in subReq:
      subFile.Status = "Done"


    subReq.Status = "Done"
    self.assertEqual( subReq.Status, "Done" )

    subReq = SubRequest()
    subReq += SubReqFile( { "Status" : "Done" } )
    self.assertEqual( subReq.Status, "Done" )
    subReq += SubReqFile( { "Status" : "Waiting" } )
    self.assertEqual( subReq.Status, "Queued" )
     

## test execution
if __name__ == "__main__":
  testLoader = unittest.TestLoader()
  subRequestTests = testLoader.loadTestsFromTestCase( SubRequestTests )
  suite = unittest.TestSuite( [ subRequestTests ] )
  unittest.TextTestRunner(verbosity=3).run(suite)

