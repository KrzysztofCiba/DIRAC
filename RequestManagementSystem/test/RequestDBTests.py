########################################################################
# $HeadURL $
# File: RequestDBTests.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2012/12/19 20:22:16
########################################################################

""" :mod: RequestDBTests 
    =======================
 
    .. module: RequestDBTests
    :synopsis: unittest for RequestDB 
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    unittest for RequestDB 
"""

__RCSID__ = "$Id $"

##
# @file RequestDBTests.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2012/12/19 20:22:29
# @brief Definition of RequestDBTests class.

## imports 
import unittest
## from DIRAC
from DIRAC.RequestManegementSystem.Client.Request import Request
from DIRAC.RequestManegementSystem.Client.Operation import Operation
from DIRAC.RequestManegementSystem.Client.File import File
## SUT
from DIRAC.RequestManegementSystem.DB.RequestDB import RequestDB


########################################################################
class RequestDBTests(unittest.TestCase):
  """
  .. class:: RequestDBTests
  unittest for RequestDB
  """

  def setUp( self ):
    """ test setup """
    pass

  def tearDown( self ):
    """ test tear down """
    pass

  def testTableDesc( self ):
    pass

## test suite execution 
if __name__ == "__main__":
  gTestLoader = unittest.TestLoader()
  gSuite = gTestLoader.loadTestsFromTestCase( RequestDBTests )
  gSuite = unittest.TestSuite( [ gSuite ] )
  unittest.TextTestRunner(verbosity=3).run( gSuite )
