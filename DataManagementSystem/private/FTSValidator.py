########################################################################
# $HeadURL $
# File: FTSValidator.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2013/04/08 14:28:29
########################################################################
""" :mod: FTSValidator 
    =======================
 
    .. module: FTSValidator
    :synopsis: making sure all information is in place
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    making sure all information is in place
"""

__RCSID__ = "$Id $"

##
# @file FTSValidator.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/04/08 14:28:52
# @brief Definition of FTSValidator class.

## imports 
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Utilities.DIRACSingleton import DIRACSingleton

########################################################################
class FTSValidator(object):
  """
  .. class:: FTSValidator
  
  """
  __metaclass__ = DIRACSingleton

  def __init__( self ):
    """c'tor

    :param self: self reference
    """
    pass

# # global instance
gFTSValidator = FTSValidator()
