########################################################################
# $HeadURL $
# File: SubRequestTests.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2012/08/14 14:30:20
########################################################################

""" :mod: SubRequestTests 
    =======================
 
    .. module: SubRequestTests
    :synopsis: SubRequest test cases
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    SubRequest test cases
"""

__RCSID__ = "$Id $"

##
# @file SubRequestTests.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2012/08/14 14:30:34
# @brief Definition of SubRequestTests class.

## imports 
import unittest
## from DIRAC
from DIRAC.RequestManagementSystem.Client.SubReqFile import SubRequFile
## SUT
from DIRAC.RequestManagementSystem.Client.SubRequest import SubRequest

########################################################################
class SubRequestTests(unittest.TestCase):
  """
  .. class:: SubRequestTests
  
  """

  def setUp( self ):
    """c'tor

    :param self: self reference
    """
    pass


## test execution
if __name__ == "__main__":
  testLoader = unittest.TestLoader()
  subRequestTests = testLoader.loadTestsFromTestCase( SubRequestTests )
  suite = unittest.TestSuite( [ subRequestTests ] )
  unittest.TextTestRunner(verbosity=3).run(suite)

