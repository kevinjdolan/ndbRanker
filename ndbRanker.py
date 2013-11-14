import logging
import time

from google.appengine.api import memcache

def getPercentile(
        name,
        query,
        field,
        value,
        samples=100,
        expires=600,
        minSamplesToCache=100):
    percentiler = getPercentiler(name, query, field, samples, expires, minSamplesToCache)
    return percentiler.percentile(value)

def getPercentiler(
        name,
        query,
        field,
        samples=100,
        expires=600,
        minSamplesToCache=100):
    key = 'NDBRANKER:%s' % name
    existing = memcache.get(key)
    if existing:
        return existing
    else:
        start = time.time()
        percentiler = _getPercentiler(query, field, samples)
        end = time.time()
        logging.debug("built percentiler %s in %s seconds" % (name, end-start))
        if percentiler.samples >= minSamplesToCache:
            memcache.set(key, percentiler, time=expires)
        return percentiler

def _getPercentiler(query, field, samples):
    query = query.order(field)
    count = query.count()
    samples = min(samples, count)
    percentiles = []
    for i in range(0, samples):
        percentile = i / float(samples)
        offset = int(round(percentile * (count)))
        entity = query.get(offset=offset, projection=[field])
        value = getattr(entity, field._name)
        if value is not None:
            percentiles.append(value)
    return Percentiler(percentiles)

class Percentiler(object):

    def __init__(self, percentiles):
        self.percentiles = percentiles
        self.samples = len(self.percentiles)

    def percentile(self, value):
        if value < self.percentiles[0]:
            return 0.

        for i in range(1, self.samples):
            upper = self.percentiles[i]
            if value < upper:
                lower = self.percentiles[i-1]
                position = (value - lower) / (upper - lower)
                return ((i - 1) + position) / self.samples

        return 1.0