########################################################################
# $HeadURL $
# File: StateMachineTests.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2012/09/26 11:15:37
########################################################################

""" :mod: StateMachineTests 
    =======================
 
    .. module: StateMachineTests
    :synopsis: test cases for StateMachine module
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    test cases for StateMachine module
"""

__RCSID__ = "$Id $"

##
# @file StateMachineTests.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2012/09/26 11:15:55
# @brief Definition of StateMachineTests class.

## imports 
import unittest
import weakref
## SUT
from DIRAC.Core.Utilities.StateMachine import Observable, Observer

class TestObservable( Observer ):
  """ 
  .. class:: TestObservable
  """
  __metaclass__ = Observable

  def __init__( self, testVal1=None, testVal2=None ):
    """ ctor """
    print "%s created" % self
    self.testVal1, self.testVal2 = testVal1, testVal2 

  def setTestVal1( self, newValue ):
    """ testValue setter """
    self.testVal1 = newValue

  def hidden( self, newVal2 ):
    """ hidden change """
    self.testVal2 = newVal2

  def __str__(self):
    return "observable %s" % id(self)

  def notify( self, attr, oldVal, newVal, caller ):
    """ dummy notify """
    self.caller = caller
    print "%s %s.%s has been changed from %s to %s" % ( self, caller, attr, oldVal, newVal )

class TestObserver( Observer ):
  """ 
  .. class:: TestObserver
  """
  def __init__(self):
    """ c'tor """
    self.attr = None
    self.oldVal = None
    self.newVal = None
    self.caller = None
    self.called = 0
    print "%s created" % self

  def notify( self, attr, oldVal, newVal, caller ):
    """ dummy notify """
    self.attr = attr
    self.oldVal = oldVal
    self.newVal = newVal
    self.caller = caller
    self.called += 1
    print "%s %s.%s has been changed from %s to %s" % ( self, caller, attr, oldVal, newVal )

  def __str__(self):
    return "observer %s" % id(self)

  def __del__(self):
    print "%s destroyed" % self

class ObserverTests( unittest.TestCase ):
  """
  .. class:: ObserverTests
  """
  def setUp( self ):
    """ test set up """
    self.observable = TestObservable(10)

  def tearDown( self ):
    """ tear down """
    del self.observable
  
  def testExternalObserver( self ):
    """ (un)register observer, notify """
    ## cretae observer
    observer = TestObserver()
    
    ## observers - weakref.WeakValueDictionary
    self.assertEqual( isinstance( self.observable.observers(), weakref.WeakValueDictionary ), True ) 
    ## empty 
    self.assertEqual( len(self.observable.observers() ), 0 )
    ## bad observer type
    try:
      self.observable.registerObserver( 1, "doesntMatter" )
    except Exception, err:
      self.assertEqual( isinstance( err, TypeError ), True ) 
      self.assertEqual( str(err), "registerObserver: supplied argument for observer should be inherited from Observer class")

    ## unknown attribute
    try:
      self.observable.registerObserver( observer, "missingAttr" )
    except Exception, err:
      self.assertEqual( isinstance( err, AttributeError), True )
      self.assertEqual( str(err), "'TestObservable' object has no attribute 'missingAttr'" )
      ## no observers
      self.assertEqual( len(self.observable.observers() ), 0 )

    ## good attribute and observer
    self.observable.registerObserver( observer, "testVal1" )
    self.assertEqual( len(self.observable.observers() ), 1 )
    for key, value in self.observable.observers().items():
      self.assertEqual( key, "testVal1" )
      self.assertEqual( value, observer )

    ## same attribute same observer 
    self.observable.registerObserver( observer, "testVal1" )
    self.assertEqual( len(self.observable.observers() ), 1 )
    for key, value in self.observable.observers().items():
      self.assertEqual( key, "testVal1" )
      self.assertEqual( value, observer )

    ## different attribute
    self.observable.registerObserver( observer, "testVal2" )
    self.assertEqual( len( self.observable.observers() ), 2 )
    self.assertEqual( self.observable.observers()["testVal1"], observer )
    self.assertEqual( self.observable.observers()["testVal2"], observer )

    ## notify - not changing value 
    self.observable.setTestVal1( 10 )  
    self.assertEqual( observer.called, 0 )  
    self.assertEqual( observer.oldVal, None )
    self.assertEqual( observer.newVal, None )
    self.assertEqual( observer.attr, None )

    ## notify - changing value
    self.observable.setTestVal1( 11 )
    self.assertEqual( observer.called, 1 )  
    self.assertEqual( observer.oldVal, 10 )
    self.assertEqual( observer.newVal, 11 )
    self.assertEqual( observer.attr, "testVal1")

    ## hidden setattr
    self.observable.hidden(12)
    self.assertEqual( observer.called, 2 )  
    self.assertEqual( observer.oldVal, None )
    self.assertEqual( observer.newVal, 12 )
    self.assertEqual( observer.attr, "testVal2")

    ## unregister observer
    self.observable.unregisterObserver( observer )
    self.observable.setTestVal1( 12 )  
    self.observable.hidden(14)
    self.assertEqual( observer.called, 2 )  
    del observer

  def testInternalObserver( self ):
    """ 'internal' observer """
    observable = TestObservable()
    observable.registerObserver( observable, "testVal1" )
    observable.testVal1 = 100
    observable.unregisterObserver( observable )
    observable.testVal1 = 10
    observable.registerObserver( observable, "testVal1" )
    observable.testVal1 = 11

class StateTests( unitest.TestCase ):
  """
  .. class StateTests
  """
  def setUp( self ):
    pass

  
########################################################################
class StateMachineTests(unittest.TestCase):
  """
  .. class:: StateMachineTests
  
  """
  def setUp( self ):
    """ test setup

    :param self: self reference
    """
    pass


  
  
## test suite execution
if __name__ == "__main__":
  testLoader = unittest.TestLoader()
  suiteObserver = testLoader.loadTestsFromTestCase( ObserverTests  )  
  suite = unittest.TestSuite( [ suiteObserver ] ) 
  unittest.TextTestRunner(verbosity=3).run(suite)

