########################################################################
# $HeadURL $
# File: RemoveFile.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2013/03/25 07:44:19
########################################################################

""" :mod: RemoveFile 
    =======================
 
    .. module: RemoveFile
    :synopsis: removeFile operation handler
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    removeFile operation handler
"""

__RCSID__ = "$Id $"

##
# @file RemoveFile.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/03/25 07:44:27
# @brief Definition of RemoveFile class.

## imports 
from DIRAC import S_OK, S_ERROR, gMonitor
from DIRAC.RequestManagementSystem.private.BaseOperation import BaseOperation

########################################################################
class RemoveFile(BaseOperation):
  """
  .. class:: RemoveFile
  
  """

  def __init__( self, operation ):
    """c'tor

    :param self: self reference
    """
    ## call base class ctor
    BaseOperation.__init__(self, operation)
    ## gMonitor stuff goes here
    gMonitor.registerActivity( "FileRemovalsAttempted", "File removals attempted", 
                               "RemoveFile", "Files/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "FileRemovalsFaild", "File removals failed", 
                               "RemoveFile", "Files/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "FileRemovalsSuccesful", "File removals successful", 
                               "RemoveFile", "Files/min", gMonitor.OP_SUM )

  def __call__( self ):
    pass


