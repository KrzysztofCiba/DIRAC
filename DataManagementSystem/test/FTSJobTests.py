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

# #
# @file FTSJobTests.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/04/16 07:19:21
# @brief Definition of FTSJobTests class.

# # imports
import unittest
import uuid
# # from DIRAC
from DIRAC.DataManagementSystem.Client.FTSFile import FTSFile
# # SUT
from DIRAC.DataManagementSystem.Client.FTSJob import FTSJob


########################################################################
class FTSJobTests( unittest.TestCase ):
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

  def test01Ctor( self ):
    """ test ctor and (de-)serilisation """
    ftsJob = FTSJob()
    self.assertEqual( isinstance( ftsJob, FTSJob ), True )

    json = ftsJob.toJSON()
    self.assertEqual( json["OK"], True, "JSON serialization error" )
    self.assertEqual( type( json["Value"] ), dict, "JSON serialization value error" )

    ftsJobJSON = FTSJob( json["Value"] )
    self.assertEqual( isinstance( ftsJobJSON, FTSJob ), True, "JSON de-serialization error" )

    XML = ftsJob.toXML()
    self.assertEqual( XML["OK"], True, "XML serialization error" )
    self.assertEqual( XML["Value"].tag, "ftsjob", "XML serialization error - wrong tag" )


    ftsJobXML = FTSJob.fromXML( XML["Value"] )
    self.assertEqual( ftsJobXML["OK"], True, "XML de-serialization error" )
    self.assertEqual( isinstance( ftsJobXML["Value"], FTSJob ), True, "XML de-serilization error - wrong type" )

    self.assertEqual( isinstance( ftsJobJSON, FTSJob ), True )

    ftsJob.addFile( self.fileA )
    ftsJob.addFile( self.fileB )

    self.assertEqual( len( ftsJob ), 2 )
    self.assertEqual( ftsJob.Files, 2 )
    self.assertEqual( ftsJob.Size, 19 )

    json = ftsJob.toJSON()
    ftsJobJSON = FTSJob( json["Value"] )
    self.assertEqual( isinstance( ftsJobJSON, FTSJob ), True )

    XML = ftsJob.toXML()
    ftsJobXML = FTSJob.fromXML( XML["Value"] )
    self.assertEqual( isinstance( ftsJobJSON, FTSJob ), True )

    SQL = ftsJob.toSQL()
    self.assertEqual( SQL["OK"], True, "SQL serialization error" )
    self.assertEqual( SQL["Value"].startswith( "INSERT" ), True, "SQL serialization INSERT error" )

    ftsJob.FTSJobID = 123456
    SQL = ftsJob.toSQL()
    self.assertEqual( SQL["OK"], True, "SQL serialization error" )
    self.assertEqual( SQL["Value"].startswith( "UPDATE" ), True, "SQL serialization UPDATE error" )

    print SQL["Value"]

  def test02Files( self ):
    """ FTSFiles arithmetic """
    ftsJob = FTSJob()
    ftsJob.FTSGUID = str( uuid.uuid4() )

    self.assertEqual( len( ftsJob ), 0 )
    self.assertEqual( ftsJob.Files, 0 )
    self.assertEqual( ftsJob.Size, 0 )

    ftsJob.addFile( self.fileA )
    ftsJob.addFile( self.fileB )

    self.assertEqual( len( ftsJob ), 2 )
    self.assertEqual( ftsJob.Files, 2 )
    self.assertEqual( ftsJob.Size, 19 )


# # test execution
if __name__ == "__main__":
  testLoader = unittest.TestLoader()
  suite = testLoader.loadTestsFromTestCase( FTSJobTests )
  suite = unittest.TestSuite( [ suite ] )
  unittest.TextTestRunner( verbosity = 3 ).run( suite )

