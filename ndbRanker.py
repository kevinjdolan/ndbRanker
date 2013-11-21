import math
import datetime

from google.appengine.ext import ndb
from google.appengine.ext.ndb import Future

def getPercentile(
        name,
        query,
        field,
        value,
        samples=100,
        expires=600):
    percentiler = getPercentiler(name, query, field, samples, expires)
    if percentiler:
        return percentiler.percentile(value)

def getPercentiler(
        name,
        query,
        field,
        samples=100,
        expires=600):
    existing = Percentiler.get_by_id(name)
    if existing and (datetime.datetime.now() - existing.created < datetime.timedelta(seconds=expires)):
        return existing
    else:
        return _getPercentiler(query, field, samples, name)

def _getPercentiler(query, field, samples, name):
    count = query.count(limit=samples)
    if count >= samples:
        minEntity = query.order(field).get(projection=[field])
        maxEntity = query.order(-field).get(projection=[field])
        if minEntity and maxEntity:
            minValue = getattr(minEntity, field._name)
            maxValue = getattr(maxEntity, field._name)

            futures = []
            width = (maxValue - minValue) / float(samples)
            for i in range(0, samples):
                start = i * width
                end = start + width
                rangeQuery = query.filter(field >= start)
                if i < (samples - 1):
                    rangeQuery = rangeQuery.filter(field < end)
                else:
                    rangeQuery = rangeQuery.filter(field <= end) # include the max value!
                futures.append(rangeQuery.count_async())

            # would be smart to vary the size of bands based on results, in case data is not nicely distributed...

            Future.wait_all(futures)
            histogram = [future.get_result() for future in futures]
            total = sum(histogram)
            if total > 0:
                percentiler = Percentiler(id=name)
                percentiler.compute(total, minValue, maxValue, histogram)
                percentiler.put()
                return percentiler

class Percentiler(ndb.Model):

    cdf = ndb.FloatProperty(repeated=True)
    created = ndb.DateTimeProperty()
    min = ndb.FloatProperty()
    max = ndb.FloatProperty()
    total = ndb.IntegerProperty()
    width = ndb.FloatProperty()

    def compute(self, total, min, max, histogram):
        self.total = total
        self.created = datetime.datetime.now()
        self.min = min
        self.max = max
        self.width = (max - min) / (len(histogram))

        cumulative = 0.0
        self.cdf = []
        for count in histogram:
            cumulative += count / float(self.total)
            self.cdf.append(cumulative)

    def percentile(self, value):
        offset = value - self.min
        upper = math.ceil(offset / self.width)
        lower = math.floor(offset / self.width)
        if lower < 0:
            return 0.
        elif upper >= len(self.cdf):
            return 1.0
        else:
            top = self.cdf[int(upper)]
            bottom = self.cdf[int(lower)]
            height = (top - bottom)

            lowerValue = self.min + lower * self.width

            offsetFromLower = value - lowerValue
            return bottom + (height) * ((offsetFromLower) / self.width)