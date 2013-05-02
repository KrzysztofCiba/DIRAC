########################################################################
# $HeadURL $
# File: FTSSiteTests.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2013/04/16 08:52:36
########################################################################

""" :mod: FTSSiteTests 
    ==================
 
    .. module: FTSSiteTests
    :synopsis: unittest for FTSSite class
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    unittest for FTSSite class
"""

__RCSID__ = "$Id $"

##
# @file FTSSiteTests.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/04/16 08:52:44
# @brief Definition of FTSSiteTests class.

## imports 
import unittest

########################################################################
class FTSSiteTests(unittest.TestCase):
  """
  .. class:: FTSSiteTests
  
  """

  def setUp( self ):
    """c'tor

    :param self: self reference
    """
    pass



# # test execution
if __name__ == "__main__":
  testLoader = unittest.TestLoader()
  suite = testLoader.loadTestsFromTestCase( FTSSiteTests )
  suite = unittest.TestSuite( [ suite ] )
  unittest.TextTestRunner( verbosity = 3 ).run( suite )
