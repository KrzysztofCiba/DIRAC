########################################################################
# $HeadURL $
# File: PutAndRegister.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2013/03/25 07:43:24
########################################################################

""" :mod: PutAndRegister 
    =======================
 
    .. module: PutAndRegister
    :synopsis: putAndRegister operation handler
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    putAndRegister operqation handler
"""

__RCSID__ = "$Id $"

##
# @file PutAndRegister.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/03/25 07:43:34
# @brief Definition of PutAndRegister class.

## imports 
from DIRAC import S_OK, S_ERROR, gMonitor
from DIRAC.RequestManagementSystem.private.BaseOperation import BaseOperation

########################################################################
class PutAndRegister(BAseOperation):
  """
  .. class:: PutAndRegister
  
  """

  def __init__( self, operation ):
    """c'tor

    :param self: self reference
    """
    BaseOperation.__init__(self, operation)
    pass

  def __call__( self ):
    pass



