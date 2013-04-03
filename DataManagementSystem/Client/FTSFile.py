########################################################################
# $HeadURL $
# File: FTSFile.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2013/04/02 14:03:37
########################################################################
""" :mod: FTSFile
    =============

    .. module: FTSFile
    :synopsis: class representing a single file in the FTS request
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    class representing a single file in the FTS request
"""

__RCSID__ = "$Id $"

# #
# @file FTSFile.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/04/02 14:03:54
# @brief Definition of FTSFile class.

# # imports
try:
  import xml.etree.cElementTree as ElementTree
except ImportError:
  import xml.etree.ElementTree as ElementTree
from xml.parsers.expat import ExpatError

########################################################################
class FTSFile( object ):
  """
  .. class:: FTSFile

  """

  def __init__( self ):
    """c'tor

    :param self: self reference
    """
    pass

