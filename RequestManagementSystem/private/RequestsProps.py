########################################################################
# $HeadURL $
# File: RequestsProps.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2012/08/07 13:24:32
########################################################################

""" :mod: RequestsProps 
    =======================
 
    .. module: RequestsProps
    :synopsis: Request properties
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    Request properties
"""

__RCSID__ = "$Id $"

##
# @file RequestsProps.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2012/08/07 13:24:39

## imports 


########################################################################
class Column(object):
  """
  .. class:: Column
  
  """

  def __init__( self, name, pyTypes, default=None ):
    """c'tor

    :param self: self reference
    """
    self.name = name 
    self.pyType = pyTypes

  

  

  
  
