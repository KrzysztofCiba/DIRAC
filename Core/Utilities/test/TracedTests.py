########################################################################
# $HeadURL $
# File: TracedTests.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2012/08/08 15:21:32
########################################################################

""" :mod: TracedTests 
    =======================
 
    .. module: TracedTests
    :synopsis: Traced test cases
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    Traced test cases
"""

__RCSID__ = "$Id $"

##
# @file TracedTests.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2012/08/08 15:21:44
# @brief Definition of TracedTests class.

## imports 
import unittest
## SUT
from DIRAC.Core.Utilities.Traced import Traced, TracedDict, TracedList

########################################################################
class TracedTests(unittest.TestCase):
  """
  .. class:: TracedTests
  
  """
  def setUp( self ):
    """c'tor

    :param self: self reference
    """
    pass

## test execution
if __name__ == "__main__":
  pass


