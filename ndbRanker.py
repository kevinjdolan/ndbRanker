from google.appengine.ext import ndb

# TODO: balancing would be a good idea

class NdbRankerNode(ndb.Model):

    count = ndb.IntegerProperty()
    keys = ndb.StringProperty(repeated=True)
    value = ndb.FloatProperty()

    parentLeft = ndb.BooleanProperty()
    parentNode = ndb.KeyProperty(kind='NdbRankerNode')

    left = ndb.KeyProperty(kind='NdbRankerNode')
    right = ndb.KeyProperty(kind='NdbRankerNode')

    def insert(self, key, value):
        self.count += 1
        if value == self.value:
            self.keys.append(key)
            inserted = self
        else:
            if value < self.value:
                attr = 'left'
            else:
                attr = 'right'

            node = getattr(self, attr)
            if node:
                inserted = node.get().insert(key, value)
            else:
                node = _new(self.shardId(), key, value, self, attr == 'left')
                setattr(self, attr, node.key)
                inserted = node
        self.put()
        return inserted

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

def getPercentile(shardId, value):
    (less, same, more) = getRank(shardId, value)
    return (more + same) / float(less + same + more)

def getRank(shardId, value):
    root = _root(shardId)
    return root.getRank(value)

@ndb.transactional(retries=5)
def insert(shardId, key, value):
    _remove(shardId, key)
    root = _root(shardId)
    if root is None:
        _new(shardId, key, value)
    else:
        root.insert(key, value)

@ndb.transactional(retries=5)
def remove(shardId, key):
    _remove(shardId, key)

def _getNode(shardId, key):
    return NdbRankerNode.query(ancestor=_shardKey(shardId)).filter(NdbRankerNode.keys == key).get()

def _new(shardId, key, value, parentNode=None, parentLeft=None):
    node = NdbRankerNode(
        parent=_shardKey(shardId),
        keys=[key],
        value=value,
        count=1,
        parentNode=parentNode.key if parentNode else None,
        parentLeft=parentLeft,
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