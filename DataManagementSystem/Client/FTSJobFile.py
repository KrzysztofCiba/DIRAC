########################################################################
# $HeadURL $
# File: FTSJobFile.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2013/04/08 09:28:29
########################################################################

""" :mod: FTSJobFile 
    ================
 
    .. module: FTSJobFile
    :synopsis: class representing a single file in the FTS job
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    class representing a single file in the FTS job
"""

__RCSID__ = "$Id $"

##
# @file FTSJobFile.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/04/08 09:28:45
# @brief Definition of FTSJobFile class.

## imports 


########################################################################
class FTSJobFile(object):
  """
  .. class:: FTSJobFile
  
  """

  def __init__( self, fromDict = None ):
    """c'tor

    :param self: self reference
    """
    pass

  @staticmethod
  def tableDesc():
    """ get table desc """
    return { "Fields" :
             { "FTSJobFileID" : "INTEGER NOT NULL AUTO_INCREMENT",
               "FTSLfnID" :  "INTEGER NOT NULL",
               "FTSJobID" :  "INTEGER NOT NULL",
               "SourceSE" : "VARCHAR(128)",
               "SourceSURL" : "VARCHAR(255)",
               "TargerSE" : "VARCHAR(128)",
               "TargetSURL" : "VARCHAR(255)",
               "Status" : "ENUM( 'Submitted', 'Executing', 'Finished', 'FinishedDirty', 'Cancelled' ) DEFAULT 'Submitted'",
               "Error" : "VARCHAR(255)"  },
             "PrimaryKey" : [ "FTSJobFileID" ],
             "Indexes" : { "FTSJobID" : [ "FTSJobID" ], "FTSJobFileID" : [ "FTSJobFileID"] } }

  def __setattr__( self, name, value ):
    """ bweare of tpyos!!! """
    if not name.startswith( "_" ) and name not in dir( self ):
      raise AttributeError( "'%s' has no attribute '%s'" % ( self.__class__.__name__, name ) )
    try:
      object.__setattr__( self, name, value )
    except AttributeError, error:
      print name, value, error
