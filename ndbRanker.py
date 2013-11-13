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
            return self
        else:
            if value < self.value:
                attr = 'left'
            else:
                attr = 'right'

            node = getattr(self, attr)
            if node:
                return node.insert(key, value)
            else:
                node = _new(self.shardId(), key, value, self, attr == 'left')
                setattr(self, attr, node)
                return node

    def rank(self):
        same = self.count
        less = 0
        more = 0
        if self.left:
            less += self.left.get().count
        if self.right:
            more += self.right.get().count
        if self.parent:
            unaccounted = self.parent.get().count
            unaccounted -= (same + less + more)
            if self.parentLeft:
                more += unaccounted
            else:
                less += unaccounted
        return (less, same, more)

    def shardId(self):
        return self.key.parent().id()

def getRank(shardId, key):
    pass

@ndb.transactional(retries=5)
def insert(shardId, key, value):
    #_remove(shardId, key)
    root = _root(shardId)
    if root is None:
        node = _new(shardId, key, value)
    else:
        node = root.insert(key, value)
    return node.rank()

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
        parentNode=parentNode,
        parentLeft=parentLeft,
    )
    node.put()
    return node

def _remove(shardId, key):
    pass

def _root(shardId):
    return NdbRankerNode.query(ancestor=_shardKey(shardId)).filter(NdbRankerNode.parentNode == None).get()

def _shardKey(shardId):
    return ndb.Key('NdbRankerShard', shardId)