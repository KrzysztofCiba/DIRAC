########################################################################
# $HeadURL $
# File: FTSStrategy.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2013/04/12 13:12:07
########################################################################
""" :mod: FTSStrategy
    =================

    .. module: FTSStrategy
    :synopsis: replication strategy for FTS transfers
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    replication strategy for FTS transfers
"""

__RCSID__ = "$Id $"

# #
# @file FTSStrategy.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/04/12 13:12:20
# @brief Definition of FTSStrategy class.

# # imports
# # imports
import random
import datetime
# # from DIRAC
from DIRAC import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.Utilities.DIRACSingleton import DIRACSingleton
from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient
from DIRAC.Core.Utilities.Graph import Graph, Node, Edge
from DIRAC.Core.Utilities.LockRing import LockRing
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import Resources, getSites, getResourceTypes
# from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getStorageElementSiteMapping

class FTSGraph( Graph ):
  """
  .. class:: FTSGraph

  graph holding FTS channels (edges) and sites (nodes)
  """
  def __init__( self, name, nodes = None, edges = None ):
    """ c'tor """
    Graph.__init__( self, name, nodes, edges )

  def findChannel( self, fromSE, toSE ):
    """ find channel between :fromSE: and :toSE: """
    for edge in self.edges():
      if fromSE in edge.fromNode.SEs and toSE in edge.toNode.SEs:
        return S_OK( edge )
    return S_ERROR( "FTSGraph: unable to find FTS channel between '%s' and '%s'" % ( fromSE, toSE ) )

class LCGSite( Node ):
  """
  .. class:: LCGSite

  not too much here, inherited to change the name
  """
  def __init__( self, name, rwAttrs = None, roAttrs = None ):
    """ c'tor """
    Node.__init__( self, name, rwAttrs, roAttrs )

class FTSChannel( Edge ):
  """
  .. class:: FTSChannel
  """
  def __init__( self, fromNode, toNode, rwAttrs = None, roAttrs = None ):
    """ c'tor """
    Edge.__init__( self, fromNode, toNode, rwAttrs, roAttrs )

  @property
  def timeToStart( self ):
    """ get time to start for this channel """
    successRate = 100.0
    attempted = self.successfulAttempts + self.failedAttempts
    if attempted:
      successRate *= self.successfulAttempts / attempted
    if successRate < self.acceptableFailureRate:
      if self.distinctFailedFiles > self.acceptableFailedFiles:
        return float( "inf" )
    if self.status != "Active":
      return float( "inf" )
    transferSpeed = { "File" : self.fileput, "Throughput" : self.throughput }[self.schedulingType]
    waitingTransfers = { "File" : self.files, "Throughput" : self.size }[self.schedulingType]
    if transferSpeed:
      return waitingTransfers / float( transferSpeed )
    return 0.0

########################################################################
class FTSStrategy( object ):
  """
  .. class:: FTSStrategy

  """
  # # make it singleton
  __metaclass__ = DIRACSingleton
  # # list of supported strategies
  __supportedStrategies = [ 'Simple', 'DynamicThroughput', 'Swarm', 'MinimiseTotalWait' ]

  def __init__( self, csPath = None ):
    """c'tor

    :param self: self reference
    :param str csPath: CS path
    """
    # ## config path
    self.csPath = csPath

    self.graphLock = LockRing().getLock( "FTSGraphLock" )

    # # own sub logger
    self.log = gLogger.getSubLogger( "FTSStrategy", child = True )
    self.log.setLevel( gConfig.getValue( self.csPath + "/LogLevel", "DEBUG" ) )

    # # CS options
    self.log.info( "Supported strategies = %s" % ", ".join( self.supportedStrategies ) )
    self.sigma = gConfig.getValue( "%s/%s" % ( self.csPath, "HopSigma" ), 5 )
    self.log.info( "HopSigma = %s" % self.sigma )
    self.schedulingType = gConfig.getValue( "%s/%s" % ( self.csPath, "SchedulingType" ), "File" )
    self.log.info( "SchedulingType = %s" % self.schedulingType )
    self.activeStrategies = gConfig.getValue( "%s/%s" % ( self.csPath, "ActiveStrategies" ), ["MinimiseTotalWait"] )
    self.log.info( "ActiveStrategies = %s" % ", ".join( self.activeStrategies ) )
    self.numberOfStrategies = len( self.activeStrategies )
    self.log.info( "Number of active strategies = %s" % self.numberOfStrategies )
    self.acceptableFailureRate = gConfig.getValue( "%s/%s" % ( self.csPath, "AcceptableFailureRate" ), 75 )
    self.log.info( "AcceptableFailureRate = %s" % self.acceptableFailureRate )
    self.acceptableFailedFiles = gConfig.getValue( "%s/%s" % ( self.csPath, "AcceptableFailedFiles" ), 5 )
    self.log.info( "AcceptableFailedFiles = %s" % self.acceptableFailedFiles )
    self.rwUpdatePeriod = gConfig.getValue( "%s/%s" % ( self.csPath, "RssRWUpdatePeriod" ), 600 )
    self.log.info( "RSSUpdatePeriod = %s s" % self.rwUpdatePeriod )
    self.rwUpdatePeriod = datetime.timedelta( seconds = self.rwUpdatePeriod )
    # # chosen strategy
    self.chosenStrategy = 0
    # # fts graph
    self.ftsGraph = None
    # # timestamp for last update
    self.lastRssUpdate = datetime.datetime.now()
    # dispatcher
    self.strategyDispatcher = { "MinimiseTotalWait" : self.minimiseTotalWait,
                                "DynamicThroughput" : self.dynamicThroughput,
                                "Simple" : self.simple,
                                "Swarm" : self.swarm }

    # #  own RSS client
    self.rssClient = ResourceStatusClient()
    # # resources helper
    self.resources = Resources()

    # # create fts graph
    self.setup()
    self.log.info( "%s has been constructed" % self.__class__.__name__ )

  def setup( self, channels = None, bandwithds = None, failedFiles = None ):
    """ prepare fts graph

    :param dict channels: { channelID : { "Files" : long , Size = long, "ChannelName" : str,
                                          "Source" : str, "Destination" : str , "ChannelName" : str, "Status" : str  } }
    :param dict bandwidths: { channelID { "Throughput" : float, "Fileput" : float, "SucessfulFiles" : long, "FailedFiles" : long  } }
    :param dict failedFiles: { channelID : int }

    channelInfo { channelName : { "ChannelID" : int, "TimeToStart" : float} }
    """
    channels = channels if channels else {}
    bandwithds = bandwithds if bandwithds else {}
    failedFiles = failedFiles if failedFiles else {}

    graph = FTSGraph( "sites" )

    res = self.resources.getEligibleResources( "Storage" )
    self.log.always( res )

    # sites = getSites()
    # if not sites["OK"]:
    #  self.log.error( sites["Message"] )
    #  return sites
    # sites = sites["Value"]
    # self.log.always( sites )

    # for site in sites:
    #  ses = getResourceTypes( site, "Storage" )
    # #  if not ses["OK"]:
    #    self.log.error( ses["Message"] )
    #    return ses
    #  ses = ses["Value"]
    #  self.log.always( ses )


    sitesDict = { "OK" : False, "Message" : "TODO" }  # getStorageElementSiteMapping()
    if not sitesDict["OK"]:
      self.log.error( sitesDict["Message"] )
      return sitesDict
    sitesDict = sitesDict["Value"]

    # # create nodes
    for site, ses in sitesDict.items():
      rwDict = self.__getRWAccessForSE( ses )
      if not rwDict["OK"]:
        return rwDict
      siteName = site
      if '.' in site:
        siteName = site.split( '.' )[1]
      graph.addNode( LCGSite( siteName, { "SEs" : rwDict["Value"] } ) )
    # # channels { channelID : { "Files" : long , Size = long, "ChannelName" : str,
    # #                          "Source" : str, "Destination" : str ,
    # #                          "ChannelName" : str, "Status" : str  } }
    # # bandwidths { channelID { "Throughput" : float, "Fileput" : float,
    # #                           "SucessfulFiles" : long, "FailedFiles" : long  } }
    # # channelInfo { channelName : { "ChannelID" : int, "TimeToStart" : float} }
    for channelID, channelDict in channels.items():
      sourceName = channelDict["Source"]
      destName = channelDict["Destination"]
      fromNode = graph.getNode( sourceName )
      toNode = graph.getNode( destName )
      if fromNode and toNode:
        rwAttrs = { "status" : channels[channelID]["Status"],
                    "files" : channelDict["Files"],
                    "size" : channelDict["Size"],
                    "successfulAttempts" : bandwithds[channelID]["SuccessfulFiles"],
                    "failedAttempts" : bandwithds[channelID]["FailedFiles"],
                    "distinctFailedFiles" : failedFiles.get( channelID, 0 ),
                    "fileput" : bandwithds[channelID]["Fileput"],
                    "throughput" : bandwithds[channelID]["Throughput"] }
        roAttrs = { "channelID" : channelID,
                    "channelName" : channelDict["ChannelName"],
                    "acceptableFailureRate" : self.acceptableFailureRate,
                    "acceptableFailedFiles" : self.acceptableFailedFiles,
                    "schedulingType" : self.schedulingType }
        ftsChannel = FTSChannel( fromNode, toNode, rwAttrs, roAttrs )
        graph.addEdge( ftsChannel )
    self.lastRssUpdate = datetime.datetime.now()
    self.ftsGraph = graph
    return S_OK()

  def updateGraph( self, rwAccess = False, replicationTree = None, size = 0.0 ):
    """ update rw access for nodes (sites) and size anf files for edges (channels) """
    replicationTree = replicationTree if replicationTree else {}
    size = size if size else 0.0
    # # update nodes rw access for SEs
    if rwAccess:
      try:
        self.graphLock.acquire()
        for lcgSite in self.ftsGraph.nodes():
          rwDict = self.__getRWAccessForSE( lcgSite.SEs.keys() )
          if not rwDict["OK"]:
            continue
        lcgSite.SEs = rwDict["Value"]
      finally:
        self.graphLock.release()
    # # update channels size and files
    if replicationTree:
      try:
        self.graphLock.acquire()
        for channel in self.ftsGraph.edges():
          if channel.channelID in replicationTree:
            channel.size += size
            channel.files += 1
      finally:
        self.graphLock.release()
    return S_OK()

  def simple( self, sourceSEs, targetSEs ):
    """ simple strategy - one source, many targets

    :param list sourceSEs: list with only one sourceSE name
    :param list targetSEs: list with target SE names
    :param str lfn: logical file name
    """
    # # make targetSEs list unique
    if len( sourceSEs ) != 1:
      return S_ERROR( "simple: wrong argument supplied for sourceSEs, only one sourceSE allowed" )
    sourceSE = sourceSEs[0]
    tree = {}
    for targetSE in targetSEs:
      channel = self.ftsGraph.findChannel( sourceSE, targetSE )
      if not channel["OK"]:
        return S_ERROR( channel["Message"] )
      channel = channel["Value"]
      if not channel.fromNode.SEs[sourceSE]["read"]:
        return S_ERROR( "simple: sourceSE '%s' in banned for reading right now" % sourceSE )
      if not channel.toNode.SEs[targetSE]["write"]:
        return S_ERROR( "simple: targetSE '%s' is banned for writing right now" % targetSE )
      if channel.channelID in tree:
        return S_ERROR( "simple: unable to create replication tree, channel '%s' cannot be used twice" % \
                          channel.channelName )
      tree[channel.channelID] = { "Ancestor" : False, "SourceSE" : sourceSE,
                                  "TargetSE" : targetSE, "Strategy" : "Simple" }

    return S_OK( tree )

  def swarm( self, sourceSEs, targetSEs ):
    """ swarm strategy - one target, many sources, pick up the fastest

    :param list sourceSEs: list of source SE
    :param str targetSEs: on element list with name of target SE
    :param str lfn: logical file name
    """
    tree = {}
    channels = []
    if len( targetSEs ) > 1:
      return S_ERROR( "swarm: wrong argument supplied for targetSEs, only one targetSE allowed" )
    targetSE = targetSEs[0]
    # # find channels
    for sourceSE in sourceSEs:
      channel = self.ftsGraph.findChannel( sourceSE, targetSE )
      if not channel["OK"]:
        self.log.warn( "swarm: %s" % channel["Message"] )
        continue
      channels.append( ( sourceSE, channel["Value"] ) )
    # # exit - no channels
    if not channels:
      return S_ERROR( "swarm: unable to find FTS channels between '%s' and '%s'" % ( ",".join( sourceSEs ), targetSE ) )
    # # filter out non active channels
    channels = [ ( sourceSE, channel ) for sourceSE, channel in channels
                 if channel.fromNode.SEs[sourceSE]["read"] and channel.toNode.SEs[targetSE]["write"] and
                 channel.status == "Active" and channel.timeToStart < float( "inf" ) ]
    # # exit - no active channels
    if not channels:
      return S_ERROR( "swarm: no active channels found between %s and %s" % ( sourceSEs, targetSE ) )

    # # find min timeToStart
    minTimeToStart = float( "inf" )
    selSourceSE = selChannel = None
    for sourceSE, ftsChannel in channels:
      if ftsChannel.timeToStart < minTimeToStart:
        minTimeToStart = ftsChannel.timeToStart
        selSourceSE = sourceSE
        selChannel = ftsChannel

    if not selSourceSE:
      return S_ERROR( "swarm: no active channels found between %s and %s" % ( sourceSEs, targetSE ) )

    tree[selChannel.channelID] = { "Ancestor" : False, "SourceSE" : selSourceSE,
                                   "TargetSE" : targetSE, "Strategy" : "Swarm" }
    return S_OK( tree )

  def minimiseTotalWait( self, sourceSEs, targetSEs ):
    """ find dag minimizing start time

    :param list sourceSEs: list of avialable source SEs
    :param list targetSEs: list of target SEs
    :param str lfn: logical file name
    """
    tree = {}
    primarySources = sourceSEs
    while targetSEs:
      minTimeToStart = float( "inf" )
      channels = []
      for targetSE in targetSEs:
        for sourceSE in sourceSEs:
          ftsChannel = self.ftsGraph.findChannel( sourceSE, targetSE )
          if not ftsChannel["OK"]:
            self.log.warn( "minimiseTotalWait: %s" % ftsChannel["Message"] )
            continue
          ftsChannel = ftsChannel["Value"]
          channels.append( ( ftsChannel, sourceSE, targetSE ) )
      if not channels:
        msg = "minimiseTotalWait: FTS channels between %s and %s not defined" % ( ",".join( sourceSEs ),
                                                                                  ",".join( targetSEs ) )
        self.log.error( msg )
        return S_ERROR( msg )
      # # filter out already used channels
      channels = [ ( channel, sourceSE, targetSE ) for channel, sourceSE, targetSE in channels
                   if channel.channelID not in tree ]
      if not channels:
        msg = "minimiseTotalWait: all FTS channels between %s and %s are already used in tree" % ( ",".join( sourceSEs ),
                                                                                                   ",".join( targetSEs ) )
        self.log.error( msg )
        return S_ERROR( msg )

      self.log.debug( "minimiseTotalWait: found %s candidate channels, checking activity" % len( channels ) )
      channels = [ ( channel, sourceSE, targetSE ) for channel, sourceSE, targetSE in channels
                   if channel.fromNode.SEs[sourceSE]["read"] and channel.toNode.SEs[targetSE]["write"]
                   and channel.status == "Active" and channel.timeToStart < float( "inf" ) ]

      if not channels:
        self.log.error( "minimiseTotalWait: no active FTS channels found" )
        return S_ERROR( "minimiseTotalWait: no active FTS channels found" )

      candidates = []
      for channel, sourceSE, targetSE in channels:
        timeToStart = channel.timeToStart
        if sourceSE not in primarySources:
          timeToStart += self.sigma
        # # local found
        if channel.fromNode == channel.toNode:
          self.log.debug( "minimiseTotalWait: found local channel '%s'" % channel.channelName )
          candidates = [ ( channel, sourceSE, targetSE ) ]
          break
        if timeToStart <= minTimeToStart:
          minTimeToStart = timeToStart
          candidates = [ ( channel, sourceSE, targetSE ) ]
        elif timeToStart == minTimeToStart:
          candidates.append( ( channel, sourceSE, targetSE ) )

      if not candidates:
        return S_ERROR( "minimiseTotalWait: unable to find candidate FTS channels minimising total wait time" )

      random.shuffle( candidates )
      selChannel, selSourceSE, selTargetSE = candidates[0]
      ancestor = False
      for channelID, treeItem in tree.items():
        if selSourceSE in treeItem["DestSE"]:
          ancestor = channelID
      tree[selChannel.channelID] = { "Ancestor" : ancestor,
                                     "SourceSE" : selSourceSE,
                                     "TargetSE" : selTargetSE,
                                     "Strategy" : "MinimiseTotalWait" }
      sourceSEs.append( selTargetSE )
      targetSEs.remove( selTargetSE )

    return S_OK( tree )

  def dynamicThroughput( self, sourceSEs, targetSEs ):
    """ dynamic throughput - many sources, many targets - find dag minimizing overall throughput

    :param list sourceSEs: list of available source SE names
    :param list targetSE: list of target SE names
    :param str lfn: logical file name
    """
    tree = {}
    primarySources = sourceSEs
    timeToSite = {}
    while targetSEs:
      minTimeToStart = float( "inf" )
      channels = []
      for targetSE in targetSEs:
        for sourceSE in sourceSEs:
          ftsChannel = self.ftsGraph.findChannel( sourceSE, targetSE )
          if not ftsChannel["OK"]:
            self.log.warn( "dynamicThroughput: %s" % ftsChannel["Message"] )
            continue
          ftsChannel = ftsChannel["Value"]
          channels.append( ( ftsChannel, sourceSE, targetSE ) )
      # # no candidate channels found
      if not channels:
        msg = "dynamicThroughput: FTS channels between %s and %s are not defined" % ( ",".join( sourceSEs ),
                                                                                      ",".join( targetSEs ) )
        self.log.error( msg )
        return S_ERROR( msg )
      # # filter out already used channels
      channels = [ ( channel, sourceSE, targetSE ) for channel, sourceSE, targetSE in channels
                   if channel.channelID not in tree ]
      if not channels:
        msg = "dynamicThroughput: all FTS channels between %s and %s are already used in tree" % ( ",".join( sourceSEs ),
                                                                                                   ",".join( targetSEs ) )
        self.log.error( msg )
        return S_ERROR( msg )
      # # filter out non-active channels
      self.log.debug( "dynamicThroughput: found %s candidate channels, checking activity" % len( channels ) )
      channels = [ ( channel, sourceSE, targetSE ) for channel, sourceSE, targetSE in channels
                   if channel.fromNode.SEs[sourceSE]["read"] and channel.toNode.SEs[targetSE]["write"]
                   and channel.status == "Active" and channel.timeToStart < float( "inf" ) ]
      if not channels:
        self.log.info( "dynamicThroughput: active candidate channels not found" )
        return S_ERROR( "dynamicThroughput: no active candidate FTS channels" )

      candidates = []
      selTimeToStart = None
      for channel, sourceSE, targetSE in channels:
        timeToStart = channel.timeToStart
        if sourceSE not in primarySources:
          timeToStart += self.sigma
        if sourceSE in timeToSite:
          timeToStart += timeToSite[sourceSE]
        # # local found
        if channel.fromNode == channel.toNode:
          self.log.debug( "dynamicThroughput: found local channel '%s'" % channel.channelName )
          candidates = [ ( channel, sourceSE, targetSE ) ]
          selTimeToStart = timeToStart
          break
        if timeToStart <= minTimeToStart:
          selTimeToStart = timeToStart
          minTimeToStart = timeToStart
          candidates = [ ( channel, sourceSE, targetSE ) ]
        elif timeToStart == minTimeToStart:
          candidates.append( ( channel, sourceSE, targetSE ) )

      if not candidates:
        return S_ERROR( "dynamicThroughput: unable to find candidate FTS channels" )

      random.shuffle( candidates )
      selChannel, selSourceSE, selTargetSE = candidates[0]
      ancestor = False
      for channelID, treeItem in tree.items():
        if selSourceSE in treeItem["DestSE"]:
          ancestor = channelID
      tree[selChannel.channelID] = { "Ancestor" : ancestor,
                                     "SourceSE" : selSourceSE,
                                     "TargetSE" : selTargetSE,
                                     "Strategy" : "DynamicThroughput" }
      timeToSite[selTargetSE] = selTimeToStart
      sourceSEs.append( selTargetSE )
      targetSEs.remove( selTargetSE )

    return S_OK( tree )

  def reset( self ):
    """ reset :chosenStrategy:

    :param self: self reference
    """
    self.chosenStrategy = 0

  @property
  def supportedStrategies( self ):
    """ Get supported strategies.

    :param self: self reference
    """
    return self.__supportedStrategies

  def replicationTree( self, sourceSEs, targetSEs, size, strategy = None ):
    """ get replication tree

    :param str lfn: LFN
    :param list sourceSEs: list of sources SE names to use
    :param list targetSEs: liost of target SE names to use
    :param long size: file size
    :param str strategy: strategy name
    """
    # # update SEs rwAccess every rwUpdatePertion timedelta (default 300 s)
    now = datetime.datetime.now()
    if now - self.lastRssUpdate > self.rwUpdatePeriod:
      update = self.updateGraph( rwAccess = True )
      if not update["OK"]:
        self.log.warn( "replicationTree: unable to update FTS graph: %s" % update["Message"] )
      else:
        self.lastRssUpdate = now
    # # get strategy
    strategy = strategy if strategy else self.__selectStrategy()
    if strategy not in self.getSupportedStrategies():
      return S_ERROR( "replicationTree: unsupported strategy '%s'" % strategy )

    self.log.info( "replicationTree: strategy=%s sourceSEs=%s targetSEs=%s size=%s" % \
                     ( strategy, sourceSEs, targetSEs, size ) )
    # # fire action from dispatcher
    tree = self.strategyDispatcher[strategy]( sourceSEs, targetSEs )
    if not tree["OK"]:
      self.log.error( "replicationTree: %s" % tree["Message"] )
      return tree
    # # update graph edges
    update = self.updateGraph( replicationTree = tree["Value"], size = size )
    if not update["OK"]:
      self.log.error( "replicationTree: unable to update FTS graph: %s" % update["Message"] )
      return update
    return tree

  def __selectStrategy( self ):
    """ If more than one active strategy use one after the other.

    :param self: self reference
    """
    chosenStrategy = self.activeStrategies[self.chosenStrategy]
    self.chosenStrategy += 1
    if self.chosenStrategy == self.numberOfStrategies:
      self.chosenStrategy = 0
    return chosenStrategy

  def __getRWAccessForSE( self, seList ):
    """ get RSS R/W for :seList:

    :param list seList: SE list
    """
    rwDict = dict.fromkeys( seList )
    for se in rwDict:
      rwDict[se] = { "read" : False, "write" : False  }
    rAccess = self.rssClient.getStorageElementStatus( seList, statusType = "ReadAccess", default = 'Unknown' )
    if not rAccess["OK"]:
      self.log.error( rAccess["Message"] )
      return rAccess["Message"]
    rAccess = [ k for k, v in rAccess["Value"].items() if "ReadAccess" in v and v["ReadAccess"] in ( "Active",
                                                                                                     "Degraded" ) ]
    wAccess = self.rssClient.getStorageElementStatus( seList, statusType = "WriteAccess", default = 'Unknown' )
    if not wAccess["OK"]:
      self.log.error( wAccess["Message"] )
      return wAccess["Message"]
    wAccess = [ k for k, v in wAccess["Value"].items() if "WriteAccess" in v and v["WriteAccess"] in ( "Active",
                                                                                                       "Degraded" ) ]
    for se in rwDict:
      rwDict[se]["read"] = se in rAccess
      rwDict[se]["write"] = se in wAccess
    return S_OK( rwDict )



