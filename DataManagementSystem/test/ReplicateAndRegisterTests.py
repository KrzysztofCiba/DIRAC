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
# # from DIRAC
from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()
# # from Core
from DIRAC.Core.Utilities.Adler import fileAdler
# # from RMS and DMS
from DIRAC.RequestManagementSystem.Client.Request import Request
from DIRAC.RequestManagementSystem.Client.Operation import Operation
from DIRAC.RequestManagementSystem.Client.File import File
from DIRAC.RequestManagementSystem.Client.ReqClient import ReqClient

########################################################################
class ReplicateAndRegisterTests( unittest.TestCase ):
  """
  .. class:: ReplicateAndRegisterTests

  """
  def setUp( self ):
    """ test setup """

    self.reqName = "fullChain"

    self.fname = "/tmp/testPutAndRegister"
    self.file = open( self.fname, "w+" )
    for i in range( 100 ):
      self.file.write( str( random.randint( 0, i ) ) )
    self.file.close()

    self.size = os.stat( self.fname ).st_size
    self.checksum = fileAdler( self.fname )

    self.putFile = File()
    self.putFile.PFN = "file://" + self.fname
    self.putFile.LFN = "/lhcb/user/c/cibak" + self.fname
    self.putFile.Checksum = self.checksum
    self.putFile.ChecksumType = "adler32"
    self.putFile.Size = self.size

    self.putAndRegister = Operation()
    self.putAndRegister.Type = "PutAndRegister"
    self.putAndRegister.TargetSE = "CERN-USER"

    self.putAndRegister.addFile( self.putFile )

    self.repFile = File()
    self.repFile.LFN = self.putFile.LFN
    self.repFile.Size = self.size
    self.repFile.Checksum = self.checksum
    self.repFile.ChecksumType = "adler32"

    self.replicateAndRegister = Operation()
    self.replicateAndRegister.Type = "ReplicateAndRegister"
    self.replicateAndRegister.TargetSE = "RAL-USER,PIC-USER"
    self.replicateAndRegister.addFile( self.repFile )

    self.removeFile = Operation()
    self.removeFile.Type = "RemoveFile"
    self.removeFile.addFile( File( { "LFN": self.putFile.LFN } ) )

    self.req = Request()
    self.req.RequestName = self.reqName
    self.req.addOperation( self.putAndRegister )
    self.req.addOperation( self.replicateAndRegister )
    self.req.addOperation( self.removeFile )

    self.reqClient = ReqClient()


  def tearDown( self ):
    """ tear down """
    os.unlink( self.fname )
    del self.req
    del self.putAndRegister
    del self.replicateAndRegister
    del self.removeFile
    del self.putFile
    del self.repFile


  def test( self ):
    """ test case """
    self.reqClient.deleteRequest( self.reqName )
    self.reqClient.putRequest( self.req )


# # test execution
if __name__ == "__main__":


  testLoader = unittest.TestLoader()
  suite = testLoader.loadTestsFromTestCase( ReplicateAndRegisterTests )
  suite = unittest.TestSuite( [ suite ] )
  unittest.TextTestRunner( verbosity = 3 ).run( suite )

