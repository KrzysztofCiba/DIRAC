########################################################################
# $HeadURL $
# File: FTSReq.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2013/04/02 13:41:20
########################################################################
""" :mod: FTSReq 
    ============
 
    .. module: FTSReq
    :synopsis: class representing FTS request record
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    class representing FTS request
"""

__RCSID__ = "$Id $"

##
# @file FTSReq.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/04/02 13:41:37
# @brief Definition of FTSReq class.

## imports 


########################################################################
class FTSReq(object):
  """
  .. class:: FTSReq
  
  """

  def __init__( self ):
    """c'tor

    :param self: self reference
    """
    pass

  @property
  def SourceSE( self ):
    """ source SE getter """
    return self.__data__["SourceSE"]

  @SourceSE.setter
  def SourceSE( self, sourceSE ):
    """ source SE setter """
    self.__data__["SourceSE"] = sourceSE 
