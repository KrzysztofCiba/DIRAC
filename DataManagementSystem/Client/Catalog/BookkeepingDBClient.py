# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/DataManagementSystem/Client/Catalog/BookkeepingDBClient.py,v 1.20 2009/08/08 10:57:43 acsmith Exp $

""" Client for BookkeepingDB file catalog
"""

__RCSID__ = "$Id: BookkeepingDBClient.py,v 1.20 2009/08/08 10:57:43 acsmith Exp $"

from DIRAC  import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.ConfigurationSystem.Client import PathFinder
from DIRAC.Core.Utilities.List import breakListIntoChunks
from DIRAC.DataManagementSystem.Client.Catalog.FileCatalogueBase import FileCatalogueBase
import types, os

class BookkeepingDBClient(FileCatalogueBase):
  """ File catalog client for bookkeeping DB
  """
  def __init__(self, url=False):
    """ Constructor of the Bookkeeping catalogue client
    """
    self.splitSize = 1000
    self.name = 'BookkeepingDB'
    self.valid = True
    try:
      if not url:
        self.url = PathFinder.getServiceURL('Bookkeeping/BookkeepingManager')
      else:
        self.url = url
    except Exception,exceptionMessage:
      gLogger.exception('BookkeepingDBClient.__init__: Exception while obtaining Bookkeeping service URL.','',exceptionMessage)
      self.valid = False

  def isOK(self):
    return self.valid

  def addFile(self,lfn):
    res = self.__checkArgumentFormat(lfn)
    if not res['OK']:
      return res
    fileList = []
    for lfn, info in res['Value'].items():
      fileList.append(lfn)
    return self.__setHasReplicaFlag(fileList)

  def addReplica(self,lfn):
    res = self.__checkArgumentFormat(lfn)
    if not res['OK']:
      return res
    fileList = []
    for lfn, info in res['Value'].items():
      fileList.append(lfn)
    return self.__setHasReplicaFlag(fileList)

  def removeFile(self,path):
    res = self.__checkArgumentFormat(path)
    if not res['OK']:
      return res
    lfns = res['Value'].keys()
    res = self.__exists(lfns)
    if not res['OK']:
      return res
    lfnsToRemove = []
    successful = {}
    for lfn,exists in res['Value']['Successful'].items():
      if exists:
        lfnsToRemove.append(lfn)
      else:
        successful[lfn] = True
    res = self.__unsetHasReplicaFlag(lfnsToRemove)
    failed = res['Value']['Failed']
    successful.update(res['Value']['Successful'])
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  def removeReplica(self,lfn):
    res = self.__checkArgumentFormat(lfn)
    if not res['OK']:
      return res
    successful = {}
    for lfn, info in res['Value'].items():
      successful[lfn] = True
    resDict = {'Failed':{},'Successful':successful}
    return S_OK(resDict)

  def setReplicaStatus(self,lfn):
    res = self.__checkArgumentFormat(lfn)
    if not res['OK']:
      return res
    successful = {}  
    for lfn, info in res['Value'].items():
      successful[lfn] = True  
    resDict = {'Failed':{},'Successful':successful}
    return S_OK(resDict)

  def setReplicaHost(self,lfn):
    res = self.__checkArgumentFormat(lfn)
    if not res['OK']:
      return res
    successful = {}  
    for lfn, info in res['Value'].items():
      successful[lfn] = True  
    resDict = {'Failed':{},'Successful':successful}
    return S_OK(resDict)

  def removeDirectory(self,lfn):
    res = self.__checkArgumentFormat(lfn)
    if not res['OK']:
      return res
    successful = {}
    for lfn, info in res['Value'].items():
      successful[lfn] = True
    resDict = {'Failed':{},'Successful':successful}
    return S_OK(resDict)

  def createDirectory(self,lfn):
    res = self.__checkArgumentFormat(lfn)
    if not res['OK']:
      return res
    successful = {}
    for lfn, info in res['Value'].items():
      successful[lfn] = True
    resDict = {'Failed':{},'Successful':successful}
    return S_OK(resDict)

  def removeLink(self,lfn):
    res = self.__checkArgumentFormat(lfn)
    if not res['OK']:
      return res
    successful = {}
    for lfn, info in res['Value'].items():
      successful[lfn] = True
    resDict = {'Failed':{},'Successful':successful}
    return S_OK(resDict)

  def createLink(self,lfn):
    res = self.__checkArgumentFormat(lfn)
    if not res['OK']:
      return res
    successful = {}
    for lfn, info in res['Value'].items():
      successful[lfn] = True
    resDict = {'Failed':{},'Successful':successful}
    return S_OK(resDict)

  def exists(self,path):
    res = self.__checkArgumentFormat(path)
    if not res['OK']:
      return res
    lfns = res['Value'].keys()
    return self.__exists(lfns)

  def getFileMetadata(self,path):
    res = self.__checkArgumentFormat(path)
    if not res['OK']:
      return res
    lfns = res['Value'].keys()
    return self.__getFileMetadata(lfns)

  ################################################################
  #
  # These are the internal methods used for actual interaction with the BK service
  #

  def __checkArgumentFormat(self,path):
    if type(path) in types.StringTypes:
      urls = {path:False}
    elif type(path) == types.ListType:
      urls = {}
      for url in path:
        urls[url] = False
    elif type(path) == types.DictType:
     urls = path
    else:
      errStr = "BookkeepingDBClient.__checkArgumentFormat: Supplied path is not of the correct format."
      gLogger.error(errStr)
      return S_ERROR(errStr)
    return S_OK(urls)

  def __setHasReplicaFlag(self,lfns):
    server = RPCClient(self.url,timeout=120)
    successful = {}
    failed = {}
    for lfnList in breakListIntoChunks(lfns,self.splitSize):
      res = server.addFiles(lfnList)
      if not res['OK']:
        for lfn in lfnList:
          failed[lfn] = res['Message']
      else:
        for lfn in lfnList:
          if res['Value'].has_key(lfn):
            failed[lfn] = res['Value'][lfn]
          else:
            successful[lfn] = True
    resDict = {'Successful':successful,'Failed':failed}
    return S_OK(resDict)

  def __unsetHasReplicaFlag(self,lfns):
    server = RPCClient(self.url,timeout=120)
    successful = {}
    failed = {}
    for lfnList in breakListIntoChunks(lfns,self.splitSize):
      res = server.removeFiles(lfnList)
      if not res['OK']:
        for lfn in lfnList:
          failed[lfn] = res['Message']
      else:
        for lfn in lfnList:
          if res['Value'].has_key(lfn):
            failed[lfn] = res['Value'][lfn]
          else:
            successful[lfn] = True
    resDict = {'Successful':successful,'Failed':failed}
    return S_OK(resDict)

  def __exists(self,lfns):
    server = RPCClient(self.url,timeout=120)
    successful = {}
    failed = {}
    for lfnList in breakListIntoChunks(lfns,self.splitSize):
      res = server.exists(lfnList)
      if not res['OK']:
        for lfn in lfnList:
          failed[lfn] = res['Message']
      else:
        for lfn,exists in res['Value'].items():
          successful[lfn] = exists
    resDict = {'Successful':successful,'Failed':{}}
    return S_OK(resDict)

  def __getFileMetadata(self,lfns):
    server = RPCClient(self.url,timeout=120)
    successful = {}
    failed = {}
    for lfnList in breakListIntoChunks(lfns,self.splitSize):
      res = server.getFileMetadata(lfnList)
      if not res['OK']:
        for lfn in lfnList:
          failed[lfn] = res['Message']
      else:
        for lfn,result in res['Value'].items():
          if result in types.StringTypes:
            failed[lfn] = result
          else:
            successful[lfn] = result
    resDict = {'Successful':successful,'Failed':failed}
    return S_OK(resDict)
