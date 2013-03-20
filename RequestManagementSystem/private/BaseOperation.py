########################################################################
# $HeadURL $
# File: BaseOperation.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2013/03/13 13:48:52
########################################################################
""" :mod: BaseOperation 
    =======================
 
    .. module: BaseOperation
    :synopsis: request operation handler base class
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    request operation handler base class
    
    this should be a functor getting Operation as ctor argument
    __call__ should return S_OK/S_ERROR

"""
__RCSID__ = "$Id $"
##
# @file BaseOperation.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/03/13 13:49:02
# @brief Definition of BaseOperation class.

from DIRAC import gLogger

########################################################################
class BaseOperation(object):
  """
  .. class:: BaseOperation

  request operation handler base class
  """

  def __init__( self, operation ):
    """c'tor

    :param self: self reference
    :param Operation operation: Operation instance
    """
    ## save operation
    self.operation = operation
    ## keep request protected
    self._request = operation._parent
    ## own monitor
    self.__monitor = {}
    ## own logger
    self.log = gLogger.getSubLogger( "%s/%s/%s" % ( self._request.RequestName,
                                                    self._request.Order,
                                                    self.operation.Type ) )
  def addMark( self, name, value=1 ):
    """ gMonitor helper 
    
    :param str name: monitor name
    :param int value: monitor value
    """
    if name not in self.__monitor:
      self.__monitor.setdefault( name, 0 )
    self.__monitor[name] += value

  def __call__( self ):
    """ call me maybe 
    
    this one should return S_OK() or S_ERROR()
    """
    raise NotImplementedError("Implement me please!")
