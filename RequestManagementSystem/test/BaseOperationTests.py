########################################################################
# $HeadURL $
# File: BaseOperationTests.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2013/03/25 08:09:08
########################################################################

""" :mod: BaseOperationTests 
    =======================
 
    .. module: BaseOperationTests
    :synopsis: unittests for BaseOperation
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    unittests for BaseOperation
"""

__RCSID__ = "$Id $"

##
# @file BaseOperationTests.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/03/25 08:09:21
# @brief Definition of BaseOperationTests class.

## imports 
import unittest
from DIRAC.RequestManagementSystem.private.BaseOperation import BaseOperation

########################################################################
class BaseOperationTests(unittest.TestCase):
  """
  .. class:: BaseOperationTests
  
  """

  def setUp( self ):
    """c'tor

    :param self: self reference
    """
    self.op = Operation( )
    pass






## tests execution
if __name__ == "__main__":
  testLoader = unittest.TestLoader()
  baseOperationTests = testLoader.loadTestsFromTestCase( BaseOperationTests )
  suite = unittest.TestSuite( [ baseOperationTests ] )
  unittest.TextTestRunner(verbosity=3).run(suite)

