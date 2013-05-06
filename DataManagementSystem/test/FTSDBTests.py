########################################################################
# $HeadURL $
# File: FTSDBTests.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2013/04/18 15:22:52
########################################################################

""" :mod: FTSDBTests
    ================

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
from DIRAC.DataManagementSystem.private.FTSHistoryView import FTSHistoryView
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

    self.ftsFiles = []
    for i in range ( 100 ):
      ftsFile = FTSFile()
      ftsFile.FileID = i
      ftsFile.OperationID = 9999
      ftsFile.LFN = "/a/b/c/%d" % i
      ftsFile.Size = 10
      ftsFile.SourceSURL = "foo://source.bar.baz/%s" % ftsFile.LFN
      ftsFile.TargetSURL = "foo://target.bar.baz/%s" % ftsFile.LFN
      ftsFile.Status = "Waiting"
      self.ftsFiles.append( ftsFile )

    ses = ["CERN-USER", "RAL-USER", "PIC-USER", "GRIDKA-USER", "CNAF-USER" ]
    statuses = [ "Submitted", "Active", "Ready", "FinishedDirty" ]

    self.submitted = 0

    self.ftsJobs = []
    for i in range( 200 ):
      ftsJob = FTSJob()
      ftsJob.FTSGUID = str( uuid.uuid4() )
      ftsJob.FTSServer = "https://fts.service.org"
      ftsJob.Status = statuses[ i % len( statuses ) ]
      ftsJob.SourceSE = ses[ i % len( ses )]
      ftsJob.TargetSE = ses[ ( i + 1 ) % len( ses ) ]

      ftsFile = FTSFile()
      ftsFile.FileID = i * 100
      ftsFile.OperationID = 9999
      ftsFile.LFN = "/a/b/c/%d" % i
      ftsFile.Size = 10
      ftsFile.SourceSURL = "foo://source.bar.baz/%s" % ftsFile.LFN
      ftsFile.TargetSURL = "foo://target.bar.baz/%s" % ftsFile.LFN
      ftsFile.Status = "Waiting"

      ftsJob.addFile( ftsFile )
      self.ftsJobs.append( ftsJob )
    self.submitted = len( [ i for i in self.ftsJobs if i.Status == "Submitted" ] )

  def tearDown( self ):
    """ clean up """
    del self.ftsFiles
    del self.ftsJobs

  def test01Create( self ):
    """ test create tables and views """
    db = FTSDB()
    self.assertEqual( db._checkTables( True )["OK"], True, "tables creation error" )
    self.assertEqual( db._checkViews( True )["OK"], True, "views creation error" )

  def test02PutGetDelete( self ):
    """ put, get, delete  methods """

    db = FTSDB()

    for ftsFile in self.ftsFiles:
      put = db.putFTSFile( ftsFile )
      self.assertEqual( put["OK"], True, "putFTSFile failed" )

    for ftsJob in self.ftsJobs:
      put = db.putFTSJob( ftsJob )
      self.assertEqual( put["OK"], True, "putFTSJob failed" )


    for i in range( 1, 101 ):
      get = db.getFTSJob( i )
      self.assertEqual( get["OK"], True, "getFTSJob failed" )
      print get


    summary = db.getDBSummary()
    self.assertEqual( summary["OK"], True, "getDBSummary failed" )
    self.assertEqual( "FTSJob" in summary["Value"], True, "getDBSummary FTSJob missing" )
    self.assertEqual( "FTSFile" in summary["Value"], True, "getDBSummary FTSFile missing" )
    self.assertEqual( "FTSHistory" in summary["Value"], True, "getDBSummary FTSHistory missing" )


  def test03FTSHistory( self ):
    """ history view """
    db = FTSDB()
    ret = db.getFTSHistory()
    self.assertEqual( ret["OK"], True, "getFTSHistory failed" )
    for ftsHistory in ret["Value"]:
      self.assertEqual( isinstance( ftsHistory, FTSHistoryView ), True, "getFTSHistory wrong instance" )

  def test04GetFTSIDs( self ):
    """ get ids """
    db = FTSDB()
    ftsJobIDs = db.getFTSJobIDs( [ "Submitted" ] )
    self.assertEqual( ftsJobIDs["OK"], True, "getFTSJobIDs error" )
    self.assertEqual( len( ftsJobIDs["Value"] ), self.submitted, "getFTSJobIDs wrong value returned" )

    ftsFileIDs = db.getFTSFileIDs( ["Waiting"] )
    self.assertEqual( ftsFileIDs["OK"], True, "getFTSFileIDs error" )
    self.assertEqual( type( ftsFileIDs["Value"] ), list, "getFTSFileIDs wrong value returned" )



# # tests execution
if __name__ == "__main__":
  testLoader = unittest.TestLoader()
  suite = testLoader.loadTestsFromTestCase( FTSDBTests )
  suite = unittest.TestSuite( [ suite ] )
  unittest.TextTestRunner( verbosity = 3 ).run( suite )


