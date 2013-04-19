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
from DIRAC import gConfig
from DIRAC.DataManagementSystem.Client.FTSFile import FTSFile
from DIRAC.DataManagementSystem.Client.FTSJob import FTSJob
from DIRAC.DataManagementSystem.Client.FTSSite import FTSSite
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

    self.ftsFileList = [ FTSFile() ] * 100
    for i, ftsFile in enumerate ( self.ftsFileList ) :
      ftsFile.FileID = i
      ftsFile.LFN = "/a/b/c"
      ftsFile.Size = 10
      ftsFile.Status = "Waiting"


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
    pass



# # tests execution
if __name__ == "__main__":
  testLoader = unittest.TestLoader()
  suite = testLoader.loadTestsFromTestCase( FTSDBTests )
  suite = unittest.TestSuite( [ suite ] )
  unittest.TextTestRunner( verbosity = 3 ).run( suite )


