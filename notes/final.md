# Data-Parallel Actors

- Properties of a query serving system:
  1. data-parallel
  2. specialized for low latency
  3. batch data updates: updates are regular but can be batched, highly
     concurrent conflicting writes rarely a problem
- distribution layer: fault-tolerance, consistency, replication, load-balancing,
  elasticity
- actor model: distributing persistent mutable state: extend it with collective
  operations for parallel queries/updates

## DPA Interface

- represent a query serving system as a collection of stateful actors. Actors
  are objects encapsulating opaque parittions of data.
- abstractions handle parallel queries and updates over data stored in many
  actors

## DPA Runtime

## Dependencies

utilities

- none

interfaces

- none

datastore

- broker.Broker
- interfaces.{Row, Shard, ShardFactory, WriteQueryPlan}
- utilities:{ConsistentHash, DataStoreDescription, TableInfo, Utilities,
  ZKShardDescription}

broker

- datastore.Datastore
- interfaces
- utilities:{ConsistentHash, DataStoreDescription, TableInfo, Utilities}

coordinator

- broker.Broker
- utilities:{ConsistentHash, Broker, DataStoreDescription, TableInfo, Utilities,
  ZKShardDescription}

awscloud

- coordinator.CoordinatorCloud
- datastore.DatastoreCloud

localcloud

- awscloud.AWSDataStoreCloud
- coordinator.CoordinatorCloud
- datastore.DataStore
- interfaces.{Row, Shard, ShardFactory }

## Operators

- Scatter operator in these files:
  - datastore/ServiceDataStoreDataStore.java
  - interfaces/ShuffleReadQueryPlan, AnchoredReadQueryPlan

- Gather operator in these files:
  - interfaces/ShuffleReadQueryPlan, AnchoredReadQueryPlan
  - datastore/ServiceBrokerDataStore.java
