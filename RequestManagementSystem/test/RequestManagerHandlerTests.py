########################################################################
# $HeadURL $
# File: RequestManagerHandlerTests.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2012/12/19 10:18:23
########################################################################

""" :mod: RequestManagerHandlerTests 
    =======================
 
    .. module: RequestManagerHandlerTests
    :synopsis: unittest for RequestManagerHandler
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    unittest for RequestManagerHandler
"""

__RCSID__ = "$Id $"

##
# @file RequestManagerHandlerTests.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2012/12/19 10:18:34
# @brief Definition of RequestManagerHandlerTests class.

## imports 
import unittest

########################################################################
class RequestManagerHandlerTests(unittest.TestCase):
  """
  .. class:: RequestManagerHandlerTests
  
  """

  def setUp( self ):
    """ test setup

    :param self: self reference
    """
    pass

  def tearDown( self ):
    """ test case tear down """
    pass


## test execution
if __name__ == "__main__":
  gLoader = unittest.TestLoader()
  gSuite = gLoader.loadTestsFromTestCase(RequestManagerHandlerTests)     
  unittest.TextTestRunner(verbosity=3).run(gSuite)

