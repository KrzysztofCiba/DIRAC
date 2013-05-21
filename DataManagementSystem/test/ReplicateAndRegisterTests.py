########################################################################
# $HeadURL $
# File: ReplicateAndRegisterTests.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2013/05/13 18:38:55
########################################################################
""" :mod: ReplicateAndRegisterTests
    ===============================

    .. module: ReplicateAndRegisterTests
    :synopsis: unittest for replicateAndRegister operation handler
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    unittest for replicateAndRegister operation handler
"""
__RCSID__ = "$Id: $"
# #
# @file ReplicateAndRegisterTests.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/05/13 18:39:13
# @brief Definition of ReplicateAndRegisterTests class.

# # imports
import unittest
import random
import os
# # from Core
from DIRAC.Core.Utilities.Adler import fileAdler
# # from RMS and DMS
from DIRAC.RequestManagementSystem.Client.Request import Request
from DIRAC.RequestManagementSystem.Client.Operation import Operation
from DIRAC.RequestManagementSystem.Client.File import File

########################################################################
class ReplicateAndRegisterTests( unittest.TestCase ):
  """
  .. class:: ReplicateAndRegisterTests

  """
  def setUp( self ):
    """ test setup """
    self.fname = "/tmp/testFile"
    self.file = open( self.fname, "w+" )
    for i in range( 100 ):
      self.file.write( str( random.randint( 0, i ) ) )
    self.file.close()

    self.checksum = fileAdler( self.fname )

    putAndRegister = Operation()
    putAndRegister.Type = "PutAndRegister"


  def tearDown( self ):
    """ tear down """
    os.unlink( self.fname )


  def test( self ):
    """ test case """
    pass

# # test execution
if __name__ == "__main__":
  testLoader = unittest.TestLoader()
  suite = testLoader.loadTestsFromTestCase( ReplicateAndRegisterTests )
  suite = unittest.TestSuite( [ suite ] )
  unittest.TextTestRunner( verbosity = 3 ).run( suite )

