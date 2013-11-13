from google.appengine.ext import ndb

# TODO: balancing would be a good idea

class NdbRankerNode(ndb.Model):

    count = ndb.IntegerProperty()
    keys = ndb.StringProperty(repeated=True)
    value = ndb.FloatProperty()

    parentNode = ndb.KeyProperty(kind='NdbRankerNode')
    left = ndb.KeyProperty(kind='NdbRankerNode')
    right = ndb.KeyProperty(kind='NdbRankerNode')

    def insert(self, key, value):
        if value == self.value:
            self._increment(key)
        else:
            if value < self.value: direction = 'left'
            else: direction = 'right'
            childNode = self._insert(direction, key, value)
            childNode.insert(key, value)

    def remove(self, key):
        self.keys.remove(key)
        self.count -= 1
        self.put()
        parentNode = self.parentNode
        while parentNode is not None:
            parent = parentNode.get()
            parent.count -= 1
            parentNode = parent.parentNode
            parent.put()

    def getLess(self):
        if self.left:
            return self.left.get().count
        else:
            return 0

    def getMore(self):
        if self.right:
            return self.right.get().count
        else:
            return 0

    def getRank(self, value):
        less = self.getLess()
        more = self.getMore()
        same = self.count - less - more
        if value == self.value:
            return (less, same, more)
        elif value < self.value:
            if self.left:
                (leftLess, leftSame, leftMore) = self.left.get().getRank(value)
                return (leftLess, leftSame, leftMore + more + same)
            else:
                return (0, 0, self.count)
        else:
            if self.right:
                (rightLess, rightSame, rightMore) = self.right.get().getRank(value)
                return (rightLess + less + same, rightSame, rightMore)
            else:
                return (self.count, 0, 0)

    def shardId(self):
        return self.key.parent().id()

    def pprint(self, indent="", direction=""):
        print "%s%s:%s %s %s" % (indent, direction, self.value, self.count, self.keys)
        if self.left:
            self.left.get().pprint(indent+" ", 'L')
        if self.right:
            self.right.get().pprint(indent+" ", 'R')


    @ndb.transactional(retries=3)
    def _insert(self, direction, key, value):
        """
            Atomically insert the value, creating a new node if necessary.
        """
        self.count += 1
        node = getattr(self, direction)
        if node:
            self.put()
            return node.get()
        else:
            node = _new(self.shardId(), value, self)
            setattr(self, direction, node.key)
            self.put()
            return node

    @ndb.transactional(retries=3)
    def _increment(self, key):
        """
            Atomically increment the counter, and append the key
        """
        self.count += 1
        if key: self.keys.append(key)
        self.put()

def getPercentile(shardId, value):
    rank = getRank(shardId, value)
    if rank is not None:
        (less, same, more) = getRank(shardId, value)
        return (more + same) / float(less + same + more)

def getRank(shardId, value):
    root = _root(shardId)
    if root is not None:
        return root.getRank(value)

def insert(shardId, key, value):
    _remove(shardId, key)
    root = _root(shardId)
    if root is None:
        root = _new(shardId, value)
    root.insert(key, value)

def remove(shardId, key):
    _remove(shardId, key)

def _getNode(shardId, key):
    return NdbRankerNode.query(ancestor=_shardKey(shardId)).filter(NdbRankerNode.keys == key).get()

def _new(shardId, value, parentNode=None):
    node = NdbRankerNode(
        parent=_shardKey(shardId),
        value=value,
        count=0,
        parentNode=parentNode.key if parentNode else None,
    )
    node.put()
    return node

def _remove(shardId, key):
    node = _getNode(shardId, key)
    if node:
        node.remove(key)

def _root(shardId):
    return NdbRankerNode.query(ancestor=_shardKey(shardId)).filter(NdbRankerNode.parentNode == None).get()

def _shardKey(shardId):
    return ndb.Key('NdbRankerShard', shardId)