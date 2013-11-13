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
            ndbRanker.insert()


    def tearDown(self):
        self.testbed.deactivate()

if __name__ == '__main__':
    unittest.main()