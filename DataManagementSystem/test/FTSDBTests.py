########################################################################
# $HeadURL $
# File: FTSDBTests.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2013/04/18 15:22:52
########################################################################

""" :mod: FTSDBTests
    =======================

    .. module: FTSDBTests
    :synopsis: unittests for FTSDB
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    unittests for FTSDB
"""

__RCSID__ = "$Id $"

# #
# @file FTSDBTests.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/04/18 15:23:03
# @brief Definition of FTSDBTests class.

# # imports
import unittest
import uuid
from DIRAC import gConfig
from DIRAC.DataManagementSystem.Client.FTSFile import FTSFile
from DIRAC.DataManagementSystem.Client.FTSJob import FTSJob
# # SUT
from DIRAC.DataManagementSystem.DB.FTSDB import FTSDB

########################################################################
class FTSDBTests( unittest.TestCase ):
  """
  .. class:: FTSDBTests

  """

  def setUp( self ):
    """ test case set up """
    # ## set some defaults
    gConfig.setOptionValue( 'DIRAC/Setup', 'Test' )
    gConfig.setOptionValue( '/DIRAC/Setups/Test/DataManagement', 'Test' )
    gConfig.setOptionValue( '/Systems/DataManagement/Test/Databases/FTSDB/Host', 'localhost' )
    gConfig.setOptionValue( '/Systems/DataManagement/Test/Databases/FTSDB/DBName', 'FTSDB' )
    gConfig.setOptionValue( '/Systems/DataManagement/Test/Databases/FTSDB/User', 'Dirac' )

    self.ftsFileList = []
    for i in range ( 100 ):
      ftsFile = FTSFile()
      ftsFile.FileID = i
      ftsFile.OperationID = 9999
      ftsFile.LFN = "/a/b/c/%d" % i
      ftsFile.Size = 10
      ftsFile.SourceSURL = "foo://source.bar.baz/%s" % ftsFile.LFN
      ftsFile.TargetSURL = "foo://target.bar.baz/%s" % ftsFile.LFN
      ftsFile.Status = "Waiting"
      self.ftsFileList.append( ftsFile )

  def tearDown( self ):
    """ clean up """
    del self.ftsFileList

  def test01Create( self ):
    """ test create tables and views """
    db = FTSDB()
    self.assertEqual( db._checkTables( True )["OK"], True, "tables creation error" )
    self.assertEqual( db._checkViews( True )["OK"], True, "views creation error" )

  def test02PutGetDelete( self ):
    """ put, get, delete  methods """
    db = FTSDB()

    for ftsFile in self.ftsFileList:
      put = db.putFTSFile( ftsFile )
      self.assertEqual( put["OK"], True, "putFTSFile failed" )

    for i in range( 100 ):
      ftsJob = FTSJob()
      ftsJob.FTSGUID = str( uuid.uuid4() )
      ftsJob.FTSServer = "https://fts.service.org"
      ftsJob.Status = "Submitted"
      ftsJob.SourceSE = "CERN-USER"
      ftsJob.TargetSE = "RAL-USER"
      for ftsFile in self.ftsFileList:
        ftsJob.addFile( ftsFile )

      self.assertEqual( len( ftsJob ), len( self.ftsFileList ), "addFile error" )

    put = db.putFTSJob( ftsJob )
    self.assertEqual( put["OK"], True, "putFTSJob failed" )

  def test03historyView( self ):
    """ history view """
    db = FTSDB()
    ret = db.getFTSHistory()
    self.assertEqual( ret["OK"], True, "getFTSHistory failed" )


# # tests execution
if __name__ == "__main__":
  testLoader = unittest.TestLoader()
  suite = testLoader.loadTestsFromTestCase( FTSDBTests )
  suite = unittest.TestSuite( [ suite ] )
  unittest.TextTestRunner( verbosity = 3 ).run( suite )


