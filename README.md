# Golang Worker Observer

Worker Observer observe other workers via special table in Casssandra and 
temporary INSERT with TTL. On output you have current workerId, num in 
cluster and cluster size.

### Cassandra Table workers 

```CREATE TABLE workers (
  task_id text,
  worker_id int,
  PRIMARY KEY (task_id, worker_id)
) WITH compaction ={'class': 'LeveledCompactionStrategy'};```

