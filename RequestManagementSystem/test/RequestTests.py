########################################################################
# $HeadURL $
# File: RequestTests.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2012/07/24 10:23:40
########################################################################

""" :mod: RequestTests 
    =======================
 
    .. module: RequestTests
    :synopsis: test cases for Request class
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    test cases for Request class
"""

__RCSID__ = "$Id $"

##
# @file RequestTests.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2012/07/24 10:23:52
# @brief Definition of RequestTests class.

## imports 
import unittest

## SUT
from DIRAC.RequestManagementSystem.Client.Request import Request

########################################################################
class RequestTests(unittest.TestCase):
  """
  .. class:: RequestTests
  
  """

  def setUp( self ):
    """ set up

    :param self: self reference
    """
    pass

  def tearDown( self ):
    """ tear down """
    pass

  def testCtor( self ):
    """ c'tor """
    pass

  def testProps( self ):
    """ props """
    pass

  def testSerilisation( self ):
    """ xml and sql serialisation """
    pass

## test execution
if __name__ == "__main__":
  testLoader = unittest.TestLoader()
  suite = testLoader.loadTestsFromTestCase(RequestTests)     
  unittest.TextTestRunner(verbosity=3).run(suite)


