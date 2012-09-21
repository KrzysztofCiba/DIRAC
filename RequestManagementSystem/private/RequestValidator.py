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
from DIRAC import S_OK, S_ERROR, gConfig 
from DIRAC.RequestManagementSystem.Client.Request import Request
from DIRAC.RequestManagementSystem.Client.SubRequest import SubRequest
from DIRAC.RequestManagementSystem.Client.SubReqFile import SubReqFile

########################################################################
class RequestValidator(object):
  """
  .. class:: RequestValidator
  
  """

  operationDict = { "diset" : ( "commitRegisters", "setFileStatusForTransformation", "setJobStatusBulk",
                                "sendXMLBookkeepingReport", "setJobParameters" ),
                    "logupload" : ( "uploadLogFiles", ),
                    "register" : ( "registeFile", "reTransfer" ),
                    "removal" : ( "replicaRemoval", "removeFile", "physicalRemoval" ),
                    "transfer" : ( "replicateAndRegister", "putAndRegister" ) } 
  
  @classmethod
  def validate( request ):
    

    pass

  @staticmethod
  def RequestNameSet( request ):
    """ required attribute: RequestName """
    if not request.RequestName:
      return S_ERROR("RequestName not set")
    return S_OK()
    
  @staticmethod
  def SubRequestsSet( request ):
    """ at least one subrequest """
    if not len(request):
      return S_ERROR("SubRequests not present in request")
    return S_OK()

  @staticmethod
  def SubRequestTypeSet( request ):
    """ required attribute for subRequest: RequestType """
    for subReq in request:
      if subReq.RequestType not in cls.operationDict:
        return S_ERROR("SubRequest #%d hasn't got a proper RequestType set" % req.indexOf( subReq ) )
    return S_OK()
      
  @staticmethod
  def SubRequestOperationSet( request ):
    """ required attribute for subRequest: Operation """
    for subReq in request:
      if subReq.Operation not in reduce( tuple.__add__, [ op for op in cls.operationDict.values() ] ):
        return S_ERROR("SubRequest #%d hasn't got a proper Operation set" % req.indexOf( subReq ) )
    return S_OK()

  @classmethod
  def RequestTypeAndOperationMatch( cls, request ):
    """ """
    for subReq in request:
      if subReq.Operation not in cls.operationDict[subReq.RequestType]:
        return S_ERROR("SubRequest #%d Operation (%s) doesn't match RequestType (%s)" % ( request.indexOf( subReq ),
                                                                                          subReq.Operation,
                                                                                          subReq.RequestType ) ) 
    return S_OK()

  @staticmethod
  def FilesInSubRequest( request ):
    for subReq in request:
      if subReq.RequestType in ( "logupload", "register", "removal", "transfer" ):
        if not len( subReq ):
          return S_ERROR( "SubRequest #%d of type '%s' hasn't got files to process" % ( request.indexOf( subReq ),
                                                                                        subReq.RequestType ) )
    return S_OK()
