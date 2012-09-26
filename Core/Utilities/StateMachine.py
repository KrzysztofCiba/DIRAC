########################################################################
# $HeadURL $
# File: StateMachine.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2012/09/26 09:39:58
########################################################################

""" :mod: StateMachine 
    =======================
 
    .. module: StateMachine
    :synopsis: generic state machine 
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    generic state machine 
"""

__RCSID__ = "$Id $"

##
# @file StateMachine.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2012/09/26 09:40:12
# @brief Definition of StateMachine class.

## for WeakValuesDictionary
import weakref

class Observable( type ):
  """ 
  .. class:: Observable 

  metaclass to create observable pattern
  """
  def __new__( cls, name, bases, classdict ):     

    def observers( self ):
      """ get observers dict """
      if not hasattr( self, "__observers" ):
        setattr( self, "__observers", weakref.WeakValueDictionary() )
      return getattr( self, "__observers" )  
  
    def notifySetAttr( func ):
      """ to be applied exclusively on __setattr__ """
      def wrapper( *args, **kwargs ):
        instance, attr, newVal = args[0], args[1], args[2]
        oldVal = None
        if hasattr( instance, attr ):
          oldVal = getattr( instance, attr )
        ret = func( *args, **kwargs )
        if not oldVal:
          instance.notify( attr, "EVSET" )
        elif oldVal != newVal:
          instance.notify( attr, "EVCHG" )
        return ret
      return wrapper

    def registerObserver( self, observer, watchedAttribute ):
      """ add new :observer: """
      ## check for attribute, could raise AttributeError 
      getattr( self, watchedAttribute )
      if watchedAttribute not in self.observers():
        self.observers()[watchedAttribute] = observer
        
    def unregisterObserver( self, watchedAttribute ):
      """ remove :observer: """
      if watchedAttribute in self.observers():
        del self.observers()[watchedAttribute]

    def notify( self, attribute=None, event=None ):
      """ notify observers """
      if attribute and attribute in self.observers():
          self.observers()[attribute].notify( getattr(self, attribute), self, event )
      else:
        for attribute, observer in self.observers():
          observer.notify( getattr(self, attribute), self )

    ## add functions 
    classdict["observers"] = observers
    classdict["registerObserver"] = registerObserver
    classdict["unregisterObserver"] = unregisterObserver
    classdict["notify"] = notify
    aType = type.__new__( cls, name, bases, classdict )
    ## decorate setattr
    aType.__setattr__ = notifySetAttr( aType.__setattr__ )
    return aType 

class Observer( object ):
  """
  .. class:: Observer
  
  generic class for Observer pattern
  """
  def notify( self, attribute, observable, event=None ):
    """ callback fruntion from :observable: on :attribute: change """
    raise NotImplementedError("'notify' has to be implemented in the child class")

class State( object ):
  
  def __init__( self, stateName ):
    self.__stateName = stateName 

  def stateName( self ):
    return self.__stateName 

  def __call__( self, event ):
    self.action( event )
      
class TranstionTable( dict ):
  
  def addTransition( stateA, stateB, event, action ):
    dict.__setitem__[stateA] = ( stateB, event, action )


########################################################################
class StateMachine( Observer ):
  """
  .. class:: StateMachine
  
  """
  
  def __init__( self ):
    """c'tor

    :param self: self reference
    """
    self.transTable = TranstionTable()

    
if __name__ == "__main__":

  class A( object ):
    __metaclass__ = Observable

    a = None

    def __init__( self ):      
      self.a = 1
      
    def seta( self, val ):
      self.a = val

  class AO( Observer ):
    def notify( self, attr, caller, event ):
      print "AO notify called by %s on event %s" % ( caller, event )  

  ao = AO()
  a = A()

  a.registerObserver( ao, "a" )

  a.seta(10)
  a.unregisterObserver( "a" )

  a.seta(12)
  a.registerObserver( ao, "a" )

  a.seta(13)

