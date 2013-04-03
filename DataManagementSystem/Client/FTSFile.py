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
  
  @staticmethod
  def tabelDesc():
    """ get table desc """
    return { "Fields" :
              { "FTSFileID" : "INTEGER NOT NULL AUTO_INCREMENT",
                "FTSReqID" : "INTEGER NOT NULL",
                "LFN" : "VARCHAR(255)",
                "SourceSURL" : "VARCHAR(255)",
                "TargetSURL" : "VARCHAR(255)",
                "Checksum" : "VARCHAR(64)",
                "ChecksumType" : "VARCHAR(32)",
                "Status" : "ENUM( 'Submitted', 'Executing', 'Finished', 'FinishedDirty', 'Cancelled' ) DEFAULT 'Submitted'",
                "Error" : "VARCHAR(255)",
               "PrimaryKey" : [ "FTSFileID" ],
             "Indexes" : { "FTSFileID" : [ "FTSFileID" ] } } }

  def __setattr__( self, name, value ):
    """ bweare of tpyos!!! """
    if not name.startswith( "_" ) and name not in dir( self ):
      raise AttributeError( "'%s' has no attribute '%s'" % ( self.__class__.__name__, name ) )
    try:
      object.__setattr__( self, name, value )
    except AttributeError, error:
      print name, value, error
