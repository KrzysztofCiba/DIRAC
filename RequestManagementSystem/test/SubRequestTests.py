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
    print "setup"
    self.fromDict = { "SubRequestID" : 1,
                      "RequestType" : "transfer",
                      "Operation" : "replicateAndRegister",
                      "TargetSE" : "CERN-USER,PIC-USER",
                      "SourceSE" : "" }
    self.subFile = SubReqFile( { "LFN" : "/lhcb/user/c/cibak/testFile",
                                 "Addler" : "1234567",
                                 "Size" : 1024,
                                 "Status" : "Waiting" } )
    
  def tearDown( self ):
    """ test case tear down """
    del self.fromDict
    del self.subFile

  def testCtor( self ):
    """ test constructors and (de)serialisation """
    ## empty ctor
    self.assertEqual( isinstance( SubRequest(), SubRequest), True )
    ## using fromDict
    subReq = SubRequest( self.fromDict )
    self.assertEqual( isinstance( subReq, SubRequest), True )
    for key, value in self.fromDict.items():
      self.assertEqual( getattr(subReq, key), value )
    ## from XML
    subReq = SubRequest.fromXML( subReq.toXML() )
    self.assertEqual( isinstance( subReq, SubRequest), True )
    for key, value in self.fromDict.items():
      self.assertEqual( getattr(subReq, key), value )
    ## same with file
    subReq = SubRequest( self.fromDict )
    subReq += self.subFile
    
    subReq = SubRequest.fromXML( subReq.toXML() )
    self.assertEqual( isinstance( subReq, SubRequest), True )
    for key, value in self.fromDict.items():
      self.assertEqual( getattr(subReq, key), value )

  def testProps( self ):
    """ test properties """
    pass

  def testStatus( self ):
    """ test status """
    subReq = SubRequest( self.fromDict )
    self.subFile.Status = "Waiting"
    subReq.Status = "Done"
    #print subReq.Status
    #subReq += self.subFile
    #print subReq.Status
    #print self.subFile.Status

## test execution
if __name__ == "__main__":
  testLoader = unittest.TestLoader()
  subRequestTests = testLoader.loadTestsFromTestCase( SubRequestTests )
  suite = unittest.TestSuite( [ subRequestTests ] )
  unittest.TextTestRunner(verbosity=3).run(suite)

