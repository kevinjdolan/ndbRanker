import ndbRanker
import random
import unittest

from google.appengine.ext import ndb
from google.appengine.ext import testbed

class TestModel(ndb.Model):

    number = ndb.FloatProperty()

class TestCase(unittest.TestCase):

    def setUp(self):
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_memcache_stub()
        self.testbed.init_datastore_v3_stub()

    def testSimple(self):
        samples = range(0, 100)
        random.shuffle(samples)
        for sample in samples:
            TestModel(number=sample).put()
        self.assertAlmostEqual(0.26, ndbRanker.getPercentile('TEST', TestModel.query(), TestModel.number, 25), places=2)
        self.assertAlmostEqual(0., ndbRanker.getPercentile('TEST', TestModel.query(), TestModel.number, -10), places=2)
        self.assertAlmostEqual(0.01, ndbRanker.getPercentile('TEST', TestModel.query(), TestModel.number, 0), places=2)
        self.assertAlmostEqual(1., ndbRanker.getPercentile('TEST', TestModel.query(), TestModel.number, 200), places=2)
        self.assertAlmostEqual(1., ndbRanker.getPercentile('TEST', TestModel.query(), TestModel.number, 99), places=2)
        self.assertAlmostEqual(1., ndbRanker.getPercentile('TEST', TestModel.query(), TestModel.number, 98.999), places=2)

    def tearDown(self):
        self.testbed.deactivate()

if __name__ == '__main__':
    unittest.main()