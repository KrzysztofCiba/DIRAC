########################################################################
# $HeadURL $
# File: SubReqFileTest.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2012/08/06 13:48:54
########################################################################

""" :mod: SubReqFileTest 
    =======================
 
    .. module: SubReqFileTest
    :synopsis: test cases for SubReqFiles
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    test cases for SubReqFiles
"""

__RCSID__ = "$Id $"

##
# @file SubReqFileTest.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2012/08/06 13:49:05
# @brief Definition of SubReqFileTest class.

## imports 
import unittest
try:
  import xml.etree.cElementTree as ElementTree
except ImportError:
  import xml.etree.ElementTree
## from DIRAC
from DIRAC.RequestManagementSystem.Client.SubRequest import SubRequest
## SUT
from DIRAC.RequestManagementSystem.Client.SubReqFile import SubReqFile

########################################################################
class SubReqFileTests( unittest.TestCase ):
  """
  .. class:: SubReqFileTest
  
  """

  def setUp( self ):
    """ test setup """
    self.fromDict = { "Size" : 1, "LFN" : "/test/lfn", "Addler" : "123456", "Status" : "Waiting" } 
    self.fileElement = ElementTree.Element( "file", self.fromDict )

  def tearDown( self ):
    """ test tear down """
    del self.fileElement
    del self.fromDict

  def test_ctors( self ):
    """ SubReqFile construction and (de)serialisation """
    ## empty defautl ctor
    subReqFile = SubReqFile()
    self.assertEqual( isinstance( subReqFile, SubReqFile), True )

    ## fromDict
    try:
      subReqFile = SubReqFile( self.fromDict )
    except AttributeError, error:
      print "AttributeError: %s" % str(error)
    self.assertEqual( isinstance( subReqFile, SubReqFile), True )
    for key, value in self.fromDict.items():
      self.assertEqual( getattr( subReqFile, key ), value  )

    ## fromXML using ElementTree
    subReqFile = SubReqFile.fromXML( self.fileElement )  
    self.assertEqual( isinstance( subReqFile, SubReqFile ), True )
    for key, value in self.fromDict.items():
      self.assertEqual( getattr( subReqFile, key ), value  )
      
  def test_props( self ):
    """ test props and attributes  """
    subReqFile = SubReqFile()
    # valid props
    subReqFile.FileID = 1
    self.assertEqual( subReqFile.FileID, 1 )
    subReqFile.SubRequestID = 1
    self.assertEqual( subReqFile.SubRequestID, 1 )
    subReqFile.Status = "Done"
    self.assertEqual( subReqFile.Status, "Done" )
    subReqFile.LFN = "/some/path/somewhere"
    self.assertEqual( subReqFile.LFN, "/some/path/somewhere" )
    subReqFile.PFN = "file:///some/path/somewhere"
    self.assertEqual( subReqFile.PFN, "file:///some/path/somewhere" )
    subReqFile.Attempt = 1
    self.assertEqual( subReqFile.Attempt, 1 )
    subReqFile.Size = 1
    self.assertEqual( subReqFile.Size, 1 )
    subReqFile.GUID = "2bbabe80-e2f1-11e1-9b23-0800200c9a66"
    self.assertEqual( subReqFile.GUID, "2bbabe80-e2f1-11e1-9b23-0800200c9a66" )
    subReqFile.Addler = "1234567"
    self.assertEqual( subReqFile.Addler, "1234567" )
    subReqFile.Md5 = "123456"
    self.assertEqual( subReqFile.Md5, "123456" )

    ## invalid props
    
    # FileID
    try:
      subReqFile.FileID = "foo"
    except Exception, error:
      self.assertEqual( isinstance( error, ValueError ), True )
    
    # SubRequestID
    try:
      subReqFile.SubRequestID = "a"
    except Exception, error:
      self.assertEqual( isinstance( error, ValueError ), True )

    # parent
    parent = SubRequest( { "SubRequestID" : 99999 } )
    parent += subReqFile
    self.assertEqual( parent.SubRequestID, subReqFile.SubRequestID )
    try:
      subReqFile.SubRequestID = 111111
    except Exception, error:
      self.assertEqual( isinstance( error, ValueError ), True )
      self.assertEqual( str(error), "Parent SubRequestID mismatch (99999 != 111111)")

    # LFN
    try:
      subReqFile.LFN = 1
    except Exception, error:
      self.assertEqual( isinstance( error, TypeError ), True )
      self.assertEqual( str(error), "LFN has to be a string!")
    try:
      subReqFile.LFN = "../some/path"
    except Exception, error:
      self.assertEqual( isinstance( error, ValueError ), True )
      self.assertEqual( str(error), "LFN should be an absolute path!")
    
    # PFN
    try:
      subReqFile.PFN = 1
    except Exception, error:
      self.assertEqual( isinstance( error, TypeError ), True )
      self.assertEqual( str(error), "PFN has to be a string!")
    try:
      subReqFile.PFN = "snafu"
    except Exception, error:
      self.assertEqual( isinstance( error, ValueError ), True )
      self.assertEqual( str(error), "Wrongly formatted URI!")
  
    # Size
    try:
      subReqFile.Size = "snafu"
    except Exception, error:
      self.assertEqual( isinstance( error, ValueError ), True )
    try:
      subReqFile.Size = -1
    except Exception, error:
      self.assertEqual( isinstance( error, ValueError ), True )
      self.assertEqual( str(error), "Size should be a positive integer!")
  
    # GUID
    try:
      subReqFile.GUID = "snafuu-uuu-uuu-uuu-uuu-u"
    except Exception, error:
      self.assertEqual( isinstance( error, ValueError ), True )
      self.assertEqual( str(error), "'snafuu-uuu-uuu-uuu-uuu-u' is not a valid GUID!")
    try:
      subReqFile.GUID = 2233345
    except Exception, error:
      self.assertEqual( isinstance( error, TypeError ), True )
      self.assertEqual( str(error), "GUID should be a string!")
      
    # Attempt
    try:
      subReqFile.Attempt = "snafu"
    except Exception, error:
      self.assertEqual( isinstance( error, ValueError ), True )
    try:
      subReqFile.Attempt = -1
    except Exception, error:
      self.assertEqual( isinstance( error, ValueError ), True )
      self.assertEqual( str(error), "Attempt should be a positive integer!")

    # Status
    try:
      subReqFile.Status = None
    except Exception, error:
      self.assertEqual( isinstance( error, ValueError ), True )
      self.assertEqual( str(error), "Unknown Status: None!")

    # Error
    try:
      subReqFile.Error = Exception("test")
    except Exception, error:
      self.assertEqual( isinstance( error, TypeError ), True )
      self.assertEqual( str(error), "Error has to be a string!")
    
## test execution
if __name__ == "__main__":
  testLoader = unittest.TestLoader()
  subReqFileTests = testLoader.loadTestsFromTestCase( SubReqFileTests )
  suite = unittest.TestSuite( [ subReqFileTests ] )
  unittest.TextTestRunner(verbosity=3).run(suite)

