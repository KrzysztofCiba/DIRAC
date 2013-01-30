########################################################################
# $HeadURL $
# File: RegisterExecutor.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2013/01/30 12:24:45
########################################################################

""" :mod: RegisterExecutor 
    ======================
 
    .. module: RegisterExecutor
    :synopsis: ReqisterExecutor
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    ReqisterExecutor
"""

__RCSID__ = "$Id $"

##
# @file RegisterExecutor.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/01/30 12:25:01
# @brief Definition of RegisterExecutor class.

## imports
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Utilities import DEncode
from DIRAC.Core.Base.ExecutorModule import ExecutorModule
from DIRAC.RequestManagementSystem.Client.Request import Request

########################################################################
class RegisterExecutor( ExecutorModule ):
  """
  .. class:: RegisterExecutor

  """
  @classmethod
  def initialize( cls ):
    """
    Executors need to know to which mind they have to connect.
    """
    cls.ex_setMind( "RequestManagement/RequestMind" )
    return S_OK()

  def processTask( self, taskId, taskData ):
    """
    This is the function that actually does the work. It receives the task,
    does the processing and sends the modified task data back.
    """
    request = taskData
    return S_OK( taskData )

  def deserializeTask( self, taskStub ):
    """ deserialise request
    
    :param str taskStub: encoded xml string
    :return: Request instance
    """
    xmlStr = DEncode.decode( taskStub )[0]
    return Request.fromXML( xmlStr )
  
  def serializeTask( self, taskData ):
    """ serialise request

    :param Request taskData: request to be serialised and sent back
    """
    taskData = taskData.toXML()
    if not taskData["OK"]:
      pass
    return DEncode.encode( taskData["Value"] )
