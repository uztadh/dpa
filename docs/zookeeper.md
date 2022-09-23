# Zookeeper

- Data Model
- Sessions
- Watches
- Consistency Guarantees

Zookeeper has a file-system like data model: there's a "root", there are paths
and at the leaves there are nodes forming a hierarchical/tree structure. Unlike
file-systems, nodes at each level can be both "files" (i.e. have associated
data) and "directories"(i.e. have children). There are a bunch of other
differences so it's not exactly a one-to-one mapping.

Znode:

- Every node in the tree is referred to as a _znode_
- each time a znode's data changes, the version number increases
- on performing an update/delete, send version of data, if version doesn't
  match, the update fails

- Znode metadata: (version number, acl changes, timestamp):
  - czxid: zxid of the change that caused this znode to be created
  - mzxid: zxid of the change that last modified this znode
  - pzxid: zxid of change that last modified the children of this znode
  - ctime: epoch time when znode was created
  - mtime: epoch time when znode was last modfied
  - version: number of changes to teh data of this znode
  - cversion: number of changes to the data of this znode
  - aversion: number of changes to the children of this znode
  - ephemeralOwner: session id of the owner of this znode if znode is ephemeral.
  - dataLength: length of the data field of this znode
  - numChildren: number of children of this znode

Ephemeral Nodes

- Znodes that exist as long as the session that created the znode is active[1]
  When the session ends, the znode is deleted. Znodes cannot have children.
- `getEphemerals`: retrieve list of ephemeral nodes created by a session per a
  given path.

Sequence Nodes

- Unique Naming.
- Implement queue - recipe

Container znodes

- special purpose znodes useful for recipes such as leader, lock etc
- When the last child of a container is deleted, the container becomes a
  candidate to be deleted by the server at some point in the future[1]
- Therefore, when creating children inside a container znode, check for
  KeeperException.NoNodeException and if it occurs, recreate the container
  znode.

TTL Nodes

Tracking time

- zxid - every change to the zookeeper state receives a stamp which exposes the
  total ordering of all changes to zookeeper. Each change has a unique zxid and
  if zxid1 is smaller than zxid2, then zxid1 happened before zxid2 [1]
- Version number, every change to a node causes an increase to one of the
  version numbers of that node:
  - version: number of hanges to the data of a znode
  - cversion: number of changes to the children of a znode
  - aversion: number of changes to the ACL of a znode
- Ticks
- Real time. Zookeeper doesn't use real time/clock time at all except to put
  timestamps into the stat structure on znode creation and znode modification.

Watches

- a watch is a trigger set by a client on a znode. Whenever the znode changes,
  the associated watch event is sent to the client depending on the kind of
  change. The trigger is usually one-time (i.e. the watch is discarded after the
  event is delivered), but in newer versions of zookeeper, it can be recurring.
  Zookeeper guarantees that a client will not see a change for which it has set
  a watch until it first sees the watch event [1]
- when a watch is set on a znode, upon changes, the client is notified and then
  the watch is cleared.

## Todo

- Add a value to a node, plus add children to a node
- Queue recipe

## References

1. ZooKeep Programmer's Guide:
   https://zookeeper.apache.org/doc/current/zookeeperProgrammers.html
