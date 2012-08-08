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

    test cases for SUbReqFiles
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
    """ SubReqFile construction """
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


    print subReqFile.toSQL()

  def test_props( self ):
    pass

## test execution
if __name__ == "__main__":
  testLoader = unittest.TestLoader()
  subReqFileTests = testLoader.loadTestsFromTestCase( SubReqFileTests )
  suite = unittest.TestSuite( [ subReqFileTests ] )
  unittest.TextTestRunner(verbosity=3).run(suite)

