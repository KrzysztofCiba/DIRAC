########################################################################
# $HeadURL $
# File: RequestExecutor.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2013/01/11 16:05:16
########################################################################
""" :mod: RequestExecutor 
    =======================
 
    .. module: RequestExecutor
    :synopsis: generic request executor
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    generic request executor
"""

__RCSID__ = "$Id $"

##
# @file RequestExecutor.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/01/11 16:05:25
# @brief Definition of RequestExecutor class.

## imports

## from DIRAC
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Utilities import DEncode
from DIRAC.Core.Base.ExecutorModule import ExecutorModule


########################################################################
class RequestExecutor(ExecutorModule):
  """
  .. class:: RequestExecutor

  request executor
  """

  def __init__( self ):
    """c'tor

    :param self: self reference
    """
    pass

  def serializeTask( self, taskData ):
    """ serialize task """
    return DEncode.encode( taskData )

  def deserializeTask( self, taskStub ):
    """ deserialize task """
    return DEncode.decode( taskStub )[0]
  
  def processTask( self , taskId, taskObj ):
    """ process task """
