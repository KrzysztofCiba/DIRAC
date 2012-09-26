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
## imports
import re
## from DIRAC
from DIRAC import S_OK, S_ERROR
########################################################################
class RequestValidator(object):
  """
  .. class:: RequestValidator
  
  This class validates newly created requests (before setting them in RequestManager) for
  required attributes.
  """

  operationDict = { "diset" : ( "commitRegisters", "setFileStatusForTransformation", "setJobStatusBulk",
                                "sendXMLBookkeepingReport", "setJobParameters" ),
                    "logupload" : ( "uploadLogFiles", ),
                    "register" : ( "registeFile", "reTransfer" ),
                    "removal" : ( "replicaRemoval", "removeFile", "physicalRemoval" ),
                    "transfer" : ( "replicateAndRegister", "putAndRegister" ) } 
  reqAttrs = { re.compile("diset") : { "SubRequest": [ "Arguments" ],  "Files" : [] },
               re.compile("transfer.putAndRegister") : { "SubRequest" : [ "TargetSE" ], "Files" : [ "LFN", "PFN" ] },
               re.compile("transfer.replicateAndRegister") : { "SubRequest" : [ "TargetSE" ], "Files" : [ "LFN" ] },
               re.compile("removal.physicalRemoval") : { "SubRequest" : ["TargetSE" ], "Files" : [ "LFN", "PFN" ] },
               re.compile("removal.removeFile") : { "SubRequest" : [], "Files" : [ "LFN" ] },
               re.compile("removal.replicaRemoval") : { "SubRequest" : [ "TargetSE" ], "Files" : [ "LFN" ] },
               re.compile("removal.reTransfer") : { "SubRequest" : [ "TargetSE" ], "Files" : [ "LFN", "PFN" ] },
               re.compile("register.registerFile") : { "SubRequest" : [], "Files" : [ "LFN", "PFN", "Size", 
                                                                                      "Adler", "GUID"] } }

  def __init__( self ):
    """ c'tor 

    just setting order of validators
    """
    self.validator = ( self.nameSet, 
                       self.hasSubRequests, 
                       self.requestTypeSet,
                       self.operationSet,
                       self.typeAndOperationMatch, 
                       self.hasFiles,
                       self.requiredAttrs )
    
  def validate( self, request ):
    """ simple validator """
    for validator in self.validator:
      isValid = validator( request )
      if not isValid["OK"]:
        return isValid
    ## if we're here request is probably valid 
    return S_OK()

  @staticmethod
  def nameSet( request ):
    """ required attribute: RequestName """
    if not request.RequestName:
      return S_ERROR("RequestName not set")
    return S_OK()
    
  @staticmethod
  def hasSubRequests( request ):
    """ at least one subrequest """
    if not len(request):
      return S_ERROR("SubRequests are not present in request '%s'" % request.RequestName )
    return S_OK()

  @classmethod
  def requestTypeSet( cls, request ):
    """ required attribute for subRequest: RequestType """
    for subReq in request:
      if subReq.RequestType not in cls.operationDict:
        return S_ERROR("SubRequest #%d hasn't got a proper RequestType set" % request.indexOf( subReq ) )
    return S_OK()
      
  @classmethod
  def operationSet( cls, request ):
    """ required attribute for subRequest: Operation """
    for subReq in request:
      if subReq.Operation not in reduce( tuple.__add__, [ op for op in cls.operationDict.values() ] ):
        return S_ERROR("SubRequest #%d hasn't got a proper Operation set" % request.indexOf( subReq ) )
    return S_OK()

  @classmethod
  def typeAndOperationMatch( cls, request ):
    """ check RequestType and Operation """
    for subReq in request:
      if subReq.Operation not in cls.operationDict[subReq.RequestType]:
        return S_ERROR("SubRequest #%d Operation (%s) doesn't match RequestType (%s)" % ( request.indexOf( subReq ),
                                                                                          subReq.Operation,
                                                                                          subReq.RequestType ) ) 
    return S_OK()

  @staticmethod
  def hasFiles( request ):
    """ check for files presence """
    for subReq in request:
      if subReq.RequestType in ( "logupload", "register", "removal", "transfer" ):
        if not len( subReq ):
          return S_ERROR( "SubRequest #%d of type '%s' hasn't got files to process" % ( request.indexOf( subReq ),
                                                                                        subReq.RequestType ) )
        if subReq.RequestType == "diset" and len( subReq ):
          return S_ERROR( "SubRequest #%d of type '%s' has got files to process" % ( request.indexOf( subReq ),
                                                                                     subReq.RequestType ) )
    return S_OK()

  @classmethod
  def requiredAttrs( cls, request ):
    """ check required attrbutes for subrequests and files """
    for subReq in request:
      for rePat, reqVal in cls.reqAttrs.items():
        if rePat.match( "%s.%s" % ( subReq.RequestType, subReq.Operation ) ):
          reqAttrs = reqVal["SubRequest"]
          fileAttrs = reqVal["Files"]
          for reqAttr in reqAttrs:
            if getattr( subReq, reqAttr ) in ( "", None ):
              return S_ERROR("SubRequest #%d of type %s and operation %s is missing %s attribute." %\
                               ( request.indexOf(subReq), subReq.RequestType, subReq.Operation, reqAttr ) )
          fileAttrs = reqVal["Files"]
          for fileAttr in fileAttrs:
            for subFile in subReq:
              if getattr( subFile, fileAttr ) in ( "", None ):
                return S_ERROR("SubRequest #%d of type %s and operation %s is missing %s attribute for file." %\
                                 ( request.indexOf(subReq), subReq.RequestType, subReq.Operation, fileAttr ) )
    return S_OK()
