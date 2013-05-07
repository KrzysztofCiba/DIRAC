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

    replication strategy for all FTS transfers

    todo: move out graph from ftsstrategy, build it standalone
"""

__RCSID__ = "$Id: $"

# #
# @file FTSStrategy.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/04/12 13:12:20
# @brief Definition of FTSStrategy class.

# # imports
import random
# # from DIRAC
from DIRAC import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.Utilities.DIRACSingleton import DIRACSingleton
from DIRAC.ResourceStatusSystem.Client.ResourceStatus import ResourceStatus
from DIRAC.Core.Utilities.Graph import Graph, Node, Edge
from DIRAC.Core.Utilities.LockRing import LockRing
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import Resources
# # from DMS
from DIRAC.DataManagementSystem.private.FTSHistoryView import FTSHistoryView

class FTSSite( Node ):
  """
  .. class:: FTSSite

  not too much here, inherited to change the name
  """
  def __init__( self, name, rwAttrs = None, roAttrs = None ):
    """ c'tor """
    Node.__init__( self, name, rwAttrs, roAttrs )

  def __contains__( self, se ):
    """ check if SE is hosted at this site """
    return se in self.SEs

class FTSRoute( Edge ):
  """
  .. class:: FTSRoute

  class representing transfers between sites
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

class FTSGraph( Graph ):
  """
  .. class:: FTSGraph

  graph holding FTS transfers (edges) and sites (nodes)
  """
  # # rss client
  __rssClient = None
  # # resources
  __resources = None

  def __init__( self,
                name,
                ftsHistoryViews = None,
                acceptableFailureRate = 0.75,
                acceptableFailedFiles = 5,
                schedulingType = "Files" ):
    """ c'tor """
    Graph.__init__( self, "FTSGraph" )
    self.acceptableFailureRate = acceptableFailureRate
    self.acceptableFailedFiles = acceptableFailedFiles
    self.schedulingType = schedulingType
    self.initialize( ftsHistoryViews )
    self.updateRWAccess()

  def initialize( self, ftsHistoryViews = None ):
    """ pass """
    ftsHistoryViews = ftsHistoryViews if ftsHistoryViews else []
    sitesDict = self.resources().getEligibleResources( "Storage" )
    if not sitesDict["OK"]:
      return sitesDict
    sitesDict = sitesDict["Value"]
    # # create nodes
    for site, ses in sitesDict.items():
      rwDict = dict.fromkeys( ses )
      for se in rwDict:
        rwDict[se] = { "read": False, "write": False }
      ftsSite = FTSSite( site, {"SEs": rwDict } )
      self.addNode( ftsSite )

    for siteA in self.nodes():
      for siteB in self.nodes():
        rwAttrs = { "files": 0, "size": 0, "successfulAttempts": 0,
                    "failedAttempts": 0, "failedSize": 0,
                    "fileput": 0.0, "throughput": 0.0 }
        roAttrs = { "routeName": "%s#%s" % ( siteA.name, siteB.name ),
                    "acceptableFailureRate": self.acceptableFailureRate,
                    "acceptableFailedFiles": self.acceptableFailedFiles,
                    "schedulingType": self.schedulingType }
        route = FTSRoute( siteA, siteB, rwAttrs, roAttrs )
        self.addEdge( route )

    for ftsHistory in ftsHistoryViews:
      sourceSE = ftsHistory.SourceSE
      targetSE = ftsHistory.TargetSE
      files = ftsHistory.Files
      failedFiles = ftsHistory.FailedFiles
      size = ftsHistory.Size
      failedSize = ftsHistory.FailedSize
      fromNode = self.findFTSSiteForSE( sourceSE )
      toNode = self.findFTSSiteForSE( targetSE )
      if not fromNode or not toNode:
        continue
      route = self.findRoute( fromNode, toNode )
      # # route is there, update
      if route["OK"]:
        route = route["Value"]
        route.files += files
        route.size += size
        route.failedSize += failedSize
        route.successfulAttempts += files - failedFiles
        route.failedAttempts += failedFiles
        route.fileput = float( route.files - route.failedFiles ) / FTSHistoryView.INTERVAL
        route.throughput = float( route.size - route.failedSize ) / FTSHistoryView.INTERVAL
      else:
        # # route is missing, create a new one
        rwAttrs = { "files": files, "size": size,
                    "successfulAttempts": files - failedFiles,
                    "failedAttempts": failedFiles,
                    "failedSize": failedSize,
                    "fileput": float( files - failedFiles ) / FTSHistoryView.INTERVAL,
                    "throughput": float( size - failedSize ) / FTSHistoryView.INTERVAL  }
        roAttrs = { "routeName": "%s#%s" % ( fromNode.name, toNode.name ),
                     "acceptableFailureRate": self.acceptableFailureRate,
                     "acceptableFailedFiles": self.acceptableFailedFiles,
                    "schedulingType": self.schedulingType }

        self.addEdge( FTSRoute( fromNode, toNode, rwAttrs, roAttrs ) )

  def rssClient( self ):
    """ RSS client getter """
    if not self.__rssClient:
      self.__rssClient = ResourceStatus()
    return self.__rssClient

  def resources( self ):
    """ resource helper getter """
    if not self.__resources:
      self.__resources = Resources()
    return self.__resources

  def updateRWAccess( self ):
    """ get RSS R/W for :seList:

    :param list seList: SE list
    """
    for site in self.nodes():
      seList = site.SEs.keys()
      rwDict = dict.fromkeys( seList )
      for se in rwDict:
        rwDict[se] = { "read": False, "write": False  }
      for se in seList:
        rAccess = self.rssClient().getStorageElementStatus( se, "ReadAccess" )
        if not rAccess["OK"]:
          return rAccess
        rwDict[se]["read"] = True if rAccess["Value"] in ( "Active", "Degraded" ) else False
        wAccess = self.rssClient().getStorageElementStatus( se, "WriteAccess" )
        if not wAccess["OK"]:
          return wAccess
        rwDict[se]["write"] = True if wAccess["Value"] in ( "Active", "Degraded" ) else False
      site.SEs = rwDict
    return S_OK()

  def findFTSSiteForSE( self, se ):
    """ return FTSSite for a given SE """
    for node in self.nodes():
      if se in node:
        return node

  def findRoute( self, fromSE, toSE ):
    """ find route between :fromSE: and :toSE: """
    for edge in self.edges():
      if fromSE in edge.fromNode.SEs and toSE in edge.toNode.SEs:
        return S_OK( edge )
    return S_ERROR( "FTSGraph: unable to find FTS route between '%s' and '%s'" % ( fromSE, toSE ) )


########################################################################
class FTSStrategy( object ):
  """
  .. class:: FTSStrategy

  helper class to create replication forrest for a given file and it's replicas using
  several different strategies
  """
  # # make it singleton
  __metaclass__ = DIRACSingleton
  # # list of supported strategies
  __supportedStrategies = [ 'Simple', 'DynamicThroughput', 'Swarm', 'MinimiseTotalWait' ]
  # # FTS graph
  __ftsGraph = None
  # # lock
  __graphLock = None
  # # resources
  __resources = None
  # # rss client
  __rssClient = None
  # # acceptable failure rate
  acceptableFailureRate = 75
  # # acceptable failed files
  acceptableFailedFiles = 5
  # # scheduling type
  schedulingType = "File"

  def __init__( self, csPath = None, ftsHistoryViews = None ):
    """c'tor

    :param self: self reference
    :param str csPath: CS path
    """
    # ## config path
    self.csPath = csPath
    # # history views
    ftsHistoryViews = ftsHistoryViews if ftsHistoryViews else []
    # # own sub logger
    self.log = gLogger.getSubLogger( "FTSStrategy", child = True )
    self.log.setLevel( gConfig.getValue( self.csPath + "/LogLevel", "DEBUG" ) )
    # # CS options
    self.log.info( "Supported strategies = %s" % ", ".join( self.supportedStrategies ) )
    self.activeStrategies = gConfig.getValue( "%s/%s" % ( self.csPath, "ActiveStrategies" ), ["MinimiseTotalWait"] )
    self.log.info( "ActiveStrategies = %s" % ", ".join( self.activeStrategies ) )
    self.numberOfStrategies = len( self.activeStrategies )
    self.log.info( "Number of active strategies = %s" % self.numberOfStrategies )
    self.sigma = gConfig.getValue( "%s/%s" % ( self.csPath, "HopSigma" ), 5 )
    self.log.info( "HopSigma = %s" % self.sigma )
    self.schedulingType = gConfig.getValue( "%s/%s" % ( self.csPath, "SchedulingType" ), "File" )
    self.log.info( "SchedulingType = %s" % self.schedulingType )
    self.acceptableFailureRate = gConfig.getValue( "%s/%s" % ( self.csPath, "AcceptableFailureRate" ), 75 )
    self.log.info( "AcceptableFailureRate = %s" % self.acceptableFailureRate )
    self.acceptableFailedFiles = gConfig.getValue( "%s/%s" % ( self.csPath, "AcceptableFailedFiles" ), 5 )
    self.log.info( "AcceptableFailedFiles = %s" % self.acceptableFailedFiles )
    # # chosen strategy
    self.chosenStrategy = 0
    # dispatcher
    self.strategyDispatcher = { "MinimiseTotalWait" : self.minimiseTotalWait,
                                "DynamicThroughput" : self.dynamicThroughput,
                                "Simple" : self.simple,
                                "Swarm" : self.swarm }

    self.ftsGraph = FTSGraph( ftsHistoryViews, self.acceptableFailureRate,
                              self.acceptableFailedFiles, self.schedulingType )

    # # if we land here everything is OK
    self.log.info( "%s has been constructed" % self.__class__.__name__ )


  @classmethod
  def graphLock( cls ):
    """ get graph lock """
    if not cls.__graphLock:
      cls.__graphLock = LockRing().getLock( "FTSGraphLock" )
    return cls.__graphLock

  @classmethod
  def resetGraph( self, ftsHistoryViews ):
    """ reset graph """
    ftsGraph = None
    try:
      self.graphLock().acquire()
      ftsGraph = FTSGraph( "FTSGraph", ftsHistoryViews, self.acceptableFailureRate,
                           self.acceptableFailedFiles, self.schedulingType )
      if ftsGraph:
        self.ftsGraph = ftsGraph
    finally:
      self.graphLock().release()
    return S_OK()

  def updateRWAccess( self ):
    """ update RW access in FTS graph """
    updateRWAccess = S_OK()
    try:
      self.graphLock().acquire()
      updateRWAccess = self.ftsGraph.updateRWAccess()
      if not updateRWAccess["OK"]:
        self.log.error( updateRWAccess["Message"] )
    finally:
      self.graphLock().release()
    return updateRWAccess

  def addTreeToGraph( self, replicationTree = None, size = 0.0 ):
    """ update rw access for nodes (sites) and size anf files for edges (channels) """
    replicationTree = replicationTree if replicationTree else {}
    size = size if size else 0.0
    if replicationTree:
      try:
        self.graphLock().acquire()
        for route in self.ftsGraph.edges():
          if route.name in replicationTree:
            route.size += size
            route.files += 1
      finally:
        self.graphLock().release()
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
      route = self.ftsGraph.findRoute( sourceSE, targetSE )
      if not route["OK"]:
        return S_ERROR( route["Message"] )
      route = route["Value"]
      if not route.fromNode.SEs[sourceSE]["read"]:
        return S_ERROR( "simple: sourceSE '%s' in banned for reading right now" % sourceSE )
      if not route.toNode.SEs[targetSE]["write"]:
        return S_ERROR( "simple: targetSE '%s' is banned for writing right now" % targetSE )
      if route.name in tree:
        return S_ERROR( "simple: unable to create replication tree, channel '%s' cannot be used twice" % \
                          route.name )
      tree[route.name] = { "Ancestor" : False, "SourceSE" : sourceSE,
                           "TargetSE" : targetSE, "Strategy" : "Simple" }

    return S_OK( tree )

  def swarm( self, sourceSEs, targetSEs ):
    """ swarm strategy - one target, many sources, pick up the fastest

    :param list sourceSEs: list of source SE
    :param str targetSEs: on element list with name of target SE
    :param str lfn: logical file name
    """
    tree = {}
    routes = []
    if len( targetSEs ) > 1:
      return S_ERROR( "swarm: wrong argument supplied for targetSEs, only one targetSE allowed" )
    targetSE = targetSEs[0]
    # # find channels
    for sourceSE in sourceSEs:
      route = self.ftsGraph.findRoute( sourceSE, targetSE )
      if not route["OK"]:
        self.log.warn( "swarm: %s" % route["Message"] )
        continue
      routes.append( ( sourceSE, route["Value"] ) )
    # # exit - no channels
    if not routes:
      return S_ERROR( "swarm: unable to find FTS channels between '%s' and '%s'" % ( ",".join( sourceSEs ), targetSE ) )
    # # filter out non active channels
    routes = [ ( sourceSE, route ) for sourceSE, route in routes
                 if route.fromNode.SEs[sourceSE]["read"] and route.toNode.SEs[targetSE]["write"] and
                 route.timeToStart < float( "inf" ) ]
    # # exit - no active channels
    if not routes:
      return S_ERROR( "swarm: no active channels found between %s and %s" % ( sourceSEs, targetSE ) )

    # # find min timeToStart
    minTimeToStart = float( "inf" )
    selSourceSE = selRoute = None
    for sourceSE, route in routes:
      if route.timeToStart < minTimeToStart:
        minTimeToStart = route.timeToStart
        selSourceSE = sourceSE
        selRoute = route

    if not selSourceSE:
      return S_ERROR( "swarm: no active channels found between %s and %s" % ( sourceSEs, targetSE ) )

    tree[selRoute.name] = { "Ancestor" : False, "SourceSE" : selSourceSE,
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
          ftsChannel = self.ftsGraph.findRoute( sourceSE, targetSE )
          if not ftsChannel["OK"]:
            self.log.warn( "minimiseTotalWait: %s" % ftsChannel["Message"] )
            continue
          ftsChannel = ftsChannel["Value"]
          channels.append( ( ftsChannel, sourceSE, targetSE ) )
      if not channels:
        msg = "minimiseTotalWait: FTS route between %s and %s not defined" % ( ",".join( sourceSEs ),
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
                   and channel.timeToStart < float( "inf" ) ]

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
      for routeName, treeItem in tree.items():
        if selSourceSE in treeItem["DestSE"]:
          ancestor = routeName
      tree[selChannel.name] = { "Ancestor" : ancestor, "SourceSE" : selSourceSE,
                                "TargetSE" : selTargetSE, "Strategy" : "MinimiseTotalWait" }
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
          ftsChannel = self.ftsGraph.findRoute( sourceSE, targetSE )
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
      self.log.debug( "dynamicThroughput: found %s candidate routes, checking activity" % len( channels ) )
      channels = [ ( channel, sourceSE, targetSE ) for channel, sourceSE, targetSE in channels
                   if channel.fromNode.SEs[sourceSE]["read"] and channel.toNode.SEs[targetSE]["write"]
                  and channel.timeToStart < float( "inf" ) ]
      if not channels:
        self.log.info( "dynamicThroughput: active candidate channels not found" )
        return S_ERROR( "dynamicThroughput: no active candidate FTS routes" )

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
          self.log.debug( "dynamicThroughput: found local route '%s'" % channel.channelName )
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
        return S_ERROR( "dynamicThroughput: unable to find candidate FTS routes" )

      random.shuffle( candidates )
      selChannel, selSourceSE, selTargetSE = candidates[0]
      ancestor = False
      for routeName, treeItem in tree.items():
        if selSourceSE in treeItem["DestSE"]:
          ancestor = routeName
      tree[selChannel.name] = { "Ancestor": ancestor, "SourceSE": selSourceSE,
                                "TargetSE": selTargetSE, "Strategy": "DynamicThroughput" }

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
    :param list targetSEs: list of target SE names to use
    :param long size: file size
    :param str strategy: strategy name
    """
    # # get strategy
    strategy = strategy if strategy else self.__selectStrategy()
    if strategy not in self.activeStrategies:
      return S_ERROR( "replicationTree: inactive or unsupported strategy '%s'" % strategy )

    self.log.info( "replicationTree: strategy=%s sourceSEs=%s targetSEs=%s size=%s" % \
                     ( strategy, sourceSEs, targetSEs, size ) )
    # # fire action from dispatcher
    tree = self.strategyDispatcher[strategy]( sourceSEs, targetSEs )
    if not tree["OK"]:
      self.log.error( "replicationTree: %s" % tree["Message"] )
      return tree
    # # update graph edges
    update = self.addTreeToGraph( replicationTree = tree["Value"], size = size )
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
