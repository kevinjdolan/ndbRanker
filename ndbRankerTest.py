import ndbRanker
import random
import unittest

from google.appengine.ext import ndb
from google.appengine.ext import testbed

class TestModel(ndb.Model):

    number = ndb.IntegerProperty(default=42)

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
            ndbRanker.insert('test', str(sample), sample)
        self.assertEqual((40, 1, 59), ndbRanker.getRank('test', 40))

    def testNotExists(self):
        samples = range(0, 10)
        random.shuffle(samples)
        for sample in samples:
            ndbRanker.insert('test', str(sample), sample)
        self.assertEqual((5, 0, 5), ndbRanker.getRank('test', 4.5))

    def testOverwrite(self):
        samples = range(0, 10)
        random.shuffle(samples)
        for sample in samples:
            ndbRanker.insert('test', str(sample), sample)
        ndbRanker.insert('test', '4', 11)
        self.assertEqual((4, 0, 6), ndbRanker.getRank('test', 4))

    def testRemovals(self):
        samples = range(0, 100)
        random.shuffle(samples)
        for sample in samples:
            ndbRanker.insert('test', str(sample), sample)
        for i in range(0, 50):
            ndbRanker.remove('test', str(2*i))
        self.assertEqual((20, 1, 29), ndbRanker.getRank('test', 41))

    def tearDown(self):
        self.testbed.deactivate()

if __name__ == '__main__':
    unittest.main()