########################################################################
# $HeadURL$
# File: RequestValidator.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2012/09/18 07:55:16
########################################################################

""" :mod: RequestValidator 
    ======================
 
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
  
  This class validates newly created requests (before saving them in RequestDB) for
  required attributes.
  """
  reqAttrs = { "ForwardDISET" : { "Operation": [ "Arguments" ],  "Files" : [] },
               "PutAndRegister" : { "Operation" : [ "TargetSE" ], "Files" : [ "LFN", "PFN" ] },
               "ReplicateAndRegister" : { "Operation" : [ "TargetSE" ], "Files" : [ "LFN" ] },
               "PhysicalRemoval" : { "Operation" : ["TargetSE" ], "Files" : [ "LFN", "PFN" ] },
               "RemoveFile" : { "Operation" : [], "Files" : [ "LFN" ] },
               "RemoveReplica" : { "Operation" : [ "TargetSE" ], "Files" : [ "LFN" ] },
               "ReTransfer" : { "Operation" : [ "TargetSE" ], "Files" : [ "LFN", "PFN" ] },
               "RegisterFile" : { "Operation" : [ ], "Files" : [ "LFN", "PFN", "Size",
                                                                "ChecksumType", "Checksum", "GUID" ] },
               "RegisterReplica" : { "Operation" : [ "TargetSE" ], "Files" : [ "LFN", "PFN" ] } }
  def __init__( self ):
    """ c'tor 

    just setting order of validators
    """
    self.validator = ( self.hasRequestName, 
                       self.hasOperations, 
                       self.hasType,
                       self.hasFiles,
                       self.hasRequiredAttrs,
                       self.hasChecksumAndChecksumType )
    
  @classmethod
  def addReqAttrsCheck( cls, operationType, operationAttrs = None, filesAttrs = None ):
    """ add required attributes of Operation of type :operationType:

    :param str operationType: Operation.Type
    :param list operationAttrs: required Operation attributes
    :param list filesAttrs: required Files attributes
    """
    toUpdate = { "Operation" : operationAttrs if operationAttrs else [],
                 "Files" : filesAttrs if filesAttrs else [] }
    if operationType not in cls.reqAttrs:
      cls.reqAttrs[operationType] = { "Operation" : [], "Files" : [] }
    for key, attrList in cls.reqAttrs[operationType].items():
      cls.reqAttrs[operationType][key] = list( set( attrList + toUpdate[key] ) )

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
    """ at least one operation is in """
    if not len(request):
      return S_ERROR("Operations not present in request '%s'" % request.RequestName )
    return S_OK()

  @staticmethod
  def hasType( request ):
    """ operation type is set """
    for operation in request:
      if not operation.Type:
        return S_ERROR("Operation #%d in request '%s' hasn't got Type set" % ( request.indexOf( operation ),
                                                                               request.RequestName ) )
    return S_OK()
      
  @classmethod
  def hasFiles( cls, request ):
    """ check for files presence """
    for operation in request:
      if operation.Type not in cls.reqAttrs:
        return S_OK()
      if cls.reqAttrs[operation.Type]["Files"] and not len( operation ):
        return S_ERROR( "Operation #%d of type '%s' hasn't got files to process." % ( request.indexOf( operation ),
                                                                                      operation.Type ) )
      if not cls.reqAttrs[operation.Type]["Files"] and len( operation ):
        return S_ERROR( "Operation #%d of type '%s' has got files to process." % ( request.indexOf( operation ),
                                                                                   operation.Type ) )
    return S_OK()

  @classmethod
  def hasRequiredAttrs( cls, request ):
    """ check required attributes for operations and files """
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

  @classmethod
  def hasChecksumAndChecksumType( cls, request ):
    """ Checksum and ChecksumType should be specified """
    for operation in request:
      for opFile in operation:
        if any( [ opFile.Checksum, opFile.ChecksumType ] ) and not all( [opFile.Checksum, opFile.ChecksumType ] ):
          return S_ERROR( "File in operation #%d is missing Checksum (%s) or ChecksumType (%s)" % \
                          ( request.indexOf(operation), opFile.Checksum, opFile.ChecksumType ) )
    return S_OK()
