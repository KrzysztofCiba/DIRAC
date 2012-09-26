########################################################################
# $HeadURL $
# File: StateMachineTests.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2012/09/26 11:15:37
########################################################################

""" :mod: StateMachineTests 
    =======================
 
    .. module: StateMachineTests
    :synopsis: test cases for StateMachine module
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    test cases for StateMachine module
"""

__RCSID__ = "$Id $"

##
# @file StateMachineTests.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2012/09/26 11:15:55
# @brief Definition of StateMachineTests class.

## imports 
import unittest

## SUT
from DIRAC.Core.Utilities.StateMachine import Observable, Observer

########################################################################
class StateMachineTests(unittest.TestCase):
  """
  .. class:: StateMachineTests
  
  """

  def setUp( self ):
    """ test setup

    :param self: self reference
    """
    pass

  

