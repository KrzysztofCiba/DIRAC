########################################################################
# $HeadURL $
# File: RemoveReplica.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2013/03/25 07:45:06
########################################################################

""" :mod: RemoveReplica 
    =======================
 
    .. module: RemoveReplica
    :synopsis: removeReplica operation handler
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    removeReplica operation handler
"""

__RCSID__ = "$Id $"

##
# @file RemoveReplica.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/03/25 07:45:17
# @brief Definition of RemoveReplica class.

## imports 
from DIRAC import S_OK, S_ERROR, gMonitor
from DIRAC.RequestManagementSystem.private.BaseOperation import BaseOperation

########################################################################
class RemoveReplica(BaseOperation):
  """
  .. class:: RemoveReplica
  
  """

  def __init__( self, operation ):
    """c'tor

    :param self: self reference
    """
    ## base class ctor
    BaseOperation.__init__( self, operation )
    ## gMonitor stuff

  def __call__(self):
    
    pass


