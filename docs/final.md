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
