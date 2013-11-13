ndbRanker
=========

Simple GAE Ranking Data Structure

Problem: You have a constantly changing NDB database of values or scores,
and you want to be able to quickly figure out the rank/percentile of a given value.

Solution: We implemented a simple order statistics tree on top of NDB
to do just that.

Everything is in a single file. **NOTE: This is a naive tree implementation, not balanced,
so don't expect guaranteed performance. We were lazy.** We'll probably implement RED/BLACK
if this proves too slow.

The data can be segmented into shards, so that you can rank multiple fields. Data is inserted
using a unique ID, which can be removed or overwritten by an additional insert.
All values are floats.

Usage:

```
import ndbRanker

# insert some documents
ndbRanker.insert('testShard', 'doc-1', 1)
ndbRanker.insert('testShard', 'doc-2', 2)
ndbRanker.insert('testShard', 'doc-3', 3)
ndbRanker.insert('testShard', 'doc-4', 4)

(less, same, more) = ndbRanker.rank('testShard', 2) # will return (1, 1, 2)
(less, same, more) = ndbRanker.rank('testShard', 2.5) # will return (2, 0, 2)
percentile = ndbRanker.rank('testShard', 2) # Will return 0.50

ndbRanker.insert('testShard', 'doc-2', 5) # will overwrite existing 'doc-2', now we have [1,3,4,5]
(less, same, more) = ndbRanker.rank('testShard', 3) # will return (1, 1, 2)
(less, same, more) = ndbRanker.rank('testShard', 2) # will return (1, 0, 3)
percentile = ndbRanker.rank('testShard', 2) # Will return 0.25, we do not interpolate!

ndbRanker.remove('testShard', 'doc-2')
(less, same, more) = ndbRanker.rank('testShard', 3) # will return (1, 1, 1)
```