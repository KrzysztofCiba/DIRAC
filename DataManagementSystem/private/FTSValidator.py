########################################################################
# $HeadURL $
# File: FTSValidator.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2013/04/08 14:28:29
########################################################################
""" :mod: FTSValidator
    ==================

    .. module: FTSValidator
    :synopsis: making sure that all required bits and pieces are in place for FTSLfn, FTSJob
               and FTSJobFile
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    A general and simple fts validator checking for required attributes and logic.
    It checks if required attributes are set/unset but not for their values.

    There is a global singleton validator for general use defined in this module: gFTSValidator.
"""

__RCSID__ = "$Id $"

# #
# @file FTSValidator.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/04/08 14:28:52
# @brief Definition of FTSValidator class.

# # imports
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities.DIRACSingleton import DIRACSingleton
from DIRAC.DataManagementSystem.Client.FTSJob import FTSJob
from DIRAC.DataManagementSystem.Client.FTSFile import FTSFile
from DIRAC.DataManagementSystem.Client.FTSSite import FTSSite

########################################################################
class FTSValidator( object ):
  """
  .. class:: FTSValidator

  """
  __metaclass__ = DIRACSingleton
  # # required attributes in FTSLfn, FTSJob and FTSJobFile
  __reqAttrs = { FTSSite: { "attrs": [ "ServerURI", "SEs" ] },
                 FTSJob: { "attrs": [ "SourceSE", "TargetSE", "FTSServer", "Size"] },
                 FTSFile: { "attrs": [ "FileID", "OperationID", "LFN", "Checksum", "ChecksumType", "Size",
                                       "SourceSE", "SourceSURL", "TargetSE", "TargetSURL" ] } }

  def __init__( self ):
    """ c'tor """
    # # order of validators
    self.validators = [ self.hasReqAttrs, self.hasFTSJobFiles ]

  def validate( self, obj ):
    """ validate

    :param mixed obj: FTSLfn, FTSJob of FTSJobFile instance
    """
    for validator in self.validators:
      isValid = validator( obj )
      if not isValid["OK"]:
        return isValid
    # # if we're here request is more or less valid
    return S_OK()

  @classmethod
  def hasReqAttrs( cls, obj ):
    """ has required attributes set

    :param mixed obj: FTSLfn, FTSJob of FTSJobFile instance
    """
    for objtype in cls.__reqAttrs:
      if isinstance( obj, objtype ):
        for attr in cls.__reqAttrs[objtype]:
          if not getattr( obj, attr ):
            return S_ERROR( "Missing property %s in %s" % ( attr, obj.__class__.__name__ ) )
      return S_OK()

  @classmethod
  def hasFTSJobFiles( cls, obj ):
    """ check if FTSJob has FTSJobFiles

    :param mixed obj: FTSLfn, FTSJob of FTSJobFile instance
    """
    if not isinstance( obj, FTSJob ):
      return S_OK()
    if not len( obj ):
      return S_ERROR( "FTSJob is missing FTSJobFiles" )
    return S_OK()

# # global instance
gFTSValidator = FTSValidator()
