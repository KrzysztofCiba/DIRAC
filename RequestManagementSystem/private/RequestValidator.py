########################################################################
# $HeadURL$
# File: RequestValidator.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2012/09/18 07:55:16
########################################################################

""" :mod: RequestValidator 
    =======================
 
    .. module: RequestValidator
    :synopsis: request validator
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    request validator
"""
__RCSID__ = "$Id$"
##
# @file RequestValidator.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2012/09/18 07:55:37
# @brief Definition of RequestValidator class.

## from DIRAC
from DIRAC import S_OK, S_ERROR
########################################################################
class RequestValidator(object):
  """
  .. class:: RequestValidator
  
  This class validates newly created requests (before setting them in RequestManager) for
  required attributes.
  """
  reqAttrs = { "diset" : { "Operation": [ "Arguments" ],  "Files" : [] },
               "putAndRegister" : { "Operation" : [ "TargetSE" ], "Files" : [ "LFN", "PFN" ] },
               "replicateAndRegister" : { "Operation" : [ "TargetSE" ], "Files" : [ "LFN" ] },
               "physicalRemoval" : { "Operation" : ["TargetSE" ], "Files" : [ "LFN", "PFN" ] },
               "removeFile" : { "Operation" : [], "Files" : [ "LFN" ] },
               "replicaRemoval" : { "Operation" : [ "TargetSE" ], "Files" : [ "LFN" ] },
               "reTransfer" : { "Operation" : [ "TargetSE" ], "Files" : [ "LFN", "PFN" ] },
               "registerFile" : { "Operation" : [], "Files" : [ "LFN", "PFN", "Size", 
                                                                "ChecksumType", "Checksum", "GUID"] } }

  def __init__( self ):
    """ c'tor 

    just setting order of validators
    """
    self.validator = ( self.hasRequestName, 
                       self.hasOperations, 
                       self.hasType,
                       self.hasFiles,
                       self.hasRequiredAttrs )
    
  def validate( self, request ):
    """ simple validator """
    for validator in self.validator:
      isValid = validator( request )
      if not isValid["OK"]:
        return isValid
    ## if we're here request is probably valid 
    return S_OK()

  @staticmethod
  def hasRequestName( request ):
    """ required attribute: RequestName """
    if not request.RequestName:
      return S_ERROR("RequestName not set")
    return S_OK()
    
  @staticmethod
  def hasOperations( request ):
    """ at least one operation """
    if not len(request):
      return S_ERROR("Operations not present in request '%s'" % request.RequestName )
    return S_OK()

  @staticmethod
  def hasType( request ):
    """ operation type set """
    for operation in request:
      if not operation.Type:
        return S_ERROR("Operation #%d in request '%s' hasn't got Type set" % ( request.indexOf( operation ),
                                                                               request.RequestName ) )
    return S_OK()
      
  @staticmethod
  def hasFiles( request ):
    """ check for files presence """
    for operation in request:
      if operation.Type in ( "putAndRegister", "replicateAndRegister", "physicalRemoval", 
                             "removeFile", "replicaRemoval", "reTransfer" ):
        if not len( operation ):
          return S_ERROR( "Operation #%d of type '%s' hasn't got files to process." % ( request.indexOf( operation ),
                                                                                        operation.Type ) )
    return S_OK()

  @classmethod
  def hasRequiredAttrs( cls, request ):
    """ check required attrbutes for operations and files """
    for operation in request:
      if operation.Type in cls.reqAttrs:
        opAttrs = cls.reqAttrs[operation.Type]["Operation"]
        for opAttr in opAttrs:
          if not getattr( operation, opAttr ):
            return S_ERROR("Operation #%d of type '%s' is missing %s attribute." %\
                             ( request.indexOf(operation), operation.Type, opAttr ) )
        fileAttrs = cls.reqAttrs[operation.Type]["Files"]
        for opFile in operation:
          for fileAttr in fileAttrs:
            if not getattr( opFile, fileAttr ):
              return S_ERROR("Operation #%d of type '%s' is missing %s attribute for file." %\
                               ( request.indexOf(operation), operation.Type, fileAttr ) )
    return S_OK()
