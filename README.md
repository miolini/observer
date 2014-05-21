Golang Worker Observer

Worker Observer observe other workers via special table in Casssandra and 
temporary INSERT with TTL. On output you have current workerId, num in 
cluster and cluster size.