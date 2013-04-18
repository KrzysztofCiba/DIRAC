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

##
# @file FTSDBTests.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/04/18 15:23:03
# @brief Definition of FTSDBTests class.

## imports 
import unittest
## SUT
from DIRAC.DataManagementSystem.DB.FTSDB import FTSDB


########################################################################
class FTSDBTests(unittest.TestCase ):
  """
  .. class:: FTSDBTests
  
  """

  def setUp( self ):
    """ test case set up """
    pass

  def tearDown( self ):
    """ clean up """
    pass

  

## tests execution
if __name__ == "__main__":
  testLoader = unittest.TestLoader()
  suite = testLoader.loadTestsFromTestCase( FTSDBTests )
  suite = unittest.TestSuite( [ suite ] )
  unittest.TextTestRunner(verbosity=3).run(suite)


