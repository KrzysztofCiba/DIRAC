########################################################################
# $HeadURL $
# File: OperationHandler.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2013/03/13 13:48:52
########################################################################
""" :mod: OperationHandler 
    =======================
 
    .. module: OperationHandler
    :synopsis: request operation handler base class
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    request operation handler base class
"""
__RCSID__ = "$Id $"
##
# @file OperationHandler.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/03/13 13:49:02
# @brief Definition of OperationHandler class.

########################################################################
class OperationHandler(object):
  """
  .. class:: OperationHandler

  request operation handler base class
  """
  def __init__( self, operation ):
    """c'tor

    :param self: self reference
    :param Operation operation: Operation instance
    """
    self.operation = operation

  def __call__( self ):
    """ call me maybe 
    
    this one should return S_OK(operation) or S_ERROR()
    """
    raise NotImplementedError("Implement me please!")
