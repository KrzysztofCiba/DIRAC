########################################################################
# $HeadURL $
# File: FTSJobTests.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2013/04/16 07:18:51
########################################################################

""" :mod: FTSJobTests 
    =======================
 
    .. module: FTSJobTests
    :synopsis: unittest for FTSJob class
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    unittest for FTSJob class
"""

__RCSID__ = "$Id $"

##
# @file FTSJobTests.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/04/16 07:19:21
# @brief Definition of FTSJobTests class.

## imports 
import unittest
## from DIRAC
from DIRAC.DataManagementSystem.Client.FTSFile import FTSFile
## SUT
from DIRAC.DataManagementSystem.Client.FTSJob import FTSJob


########################################################################
class FTSJobTests(unittest.TestCase):
  """
  .. class:: FTSJobTests
  
  """

  def setUp( self ):
    """ test set up """
    self.fileA = FTSFile( { "LFN": "/a", "ChecksumType": "ADLER32", "Checksum": "123456", "Size": 10 } )
    self.fileB = FTSFile( { "LFN": "/b", "ChecksumType": "ADLER32", "Checksum": "654321", "Size": 9 } )

  def tearDown( self ):
    """ test tear down """
    del self.fileA
    del self.fileB

  def testCtor( self ):
    """ test ctor and (de-)serilisation """
    ftsJob = FTSJob()
    self.assertEqual( isinstance( ftsJob, FTSJob ), True )

    json = ftsJob.toJSON()    
    ftsJobJSON = FTSJob( json["Value"] )
    self.assertEqual( isinstance( ftsJobJSON, FTSJob ), True )

    XML = ftsJob.toXML()
    ftsJobXML = FTSJob.fromXML( XML["Value"] )
    self.assertEqual( isinstance( ftsJobJSON, FTSJob ), True )

    ftsJob.addFile( self.fileA )
    ftsJob.addFile( self.fileB )

    self.assertEqual( len(ftsJob), 2 )
    self.assertEqual( ftsJob.NbFiles, 2 )
    self.assertEqual( ftsJob.Size, 19 )

    json = ftsJob.toJSON()    
    ftsJobJSON = FTSJob( json["Value"] )
    self.assertEqual( isinstance( ftsJobJSON, FTSJob ), True )

    XML = ftsJob.toXML()
    ftsJobXML = FTSJob.fromXML( XML["Value"] )
    self.assertEqual( isinstance( ftsJobJSON, FTSJob ), True )




## test execution
if __name__ == "__main__":

  testLoader = unittest.TestLoader()
  suite = testLoader.loadTestsFromTestCase( FTSJobTests )
  suite = unittest.TestSuite( [ suite ] )
  unittest.TextTestRunner(verbosity=3).run(suite)

