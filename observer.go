package observer

import (
    "log"
    "sync"
    "math/rand"
    "strings"
    "time"
    "github.com/gocql/gocql"
)

type WorkerObserver struct {
    mutex *sync.Mutex
    total int
    modulus int
    id int
    Ttl int
}

func NewWorkerObserver() (o *WorkerObserver) {
    o = new(WorkerObserver)
    o.mutex = new(sync.Mutex)
    o.total = 1
    o.modulus = 0
    o.id = int(rand.Int31())
    return
}

func (o *WorkerObserver) Get() (total int, modulus int) {
    total = o.total
    modulus = o.modulus
    return
}

func (o *WorkerObserver) Observe(cassandraHosts string, cassandraKeyspace string) (err error) {
    cluster := gocql.NewCluster(strings.Split(cassandraHosts,",")...)
    cluster.Keyspace = cassandraKeyspace
    cluster.NumConns = 2
    cluster.NumStreams = 8
    cluster.RetryPolicy = gocql.RetryPolicy { 3 }
    cluster.Consistency = gocql.Quorum
    session, err := cluster.CreateSession();
    if err != nil {
        return
    }
    taskId := "checker"
    localWorkerTotal := 1
    localWorkerModulus := 0
    defer session.Close()
    rand.Seed(time.Now().UnixNano())
    var workerId int
    log.Printf("worker watcher: localWorkerId = %d", o.id)
    var counter int
    for {
        err := session.Query("INSERT INTO workers (task_id, worker_id) VALUES (?, ?) USING TTL ?", taskId, o.id, o.Ttl).Consistency(gocql.Quorum).Exec()
        if err != nil {
            log.Printf("worker insert error: %s", err)
            continue
        }
        counter = 0
        iter := session.Query("SELECT worker_id FROM workers WHERE task_id = ?", "checker").Consistency(gocql.Quorum).Iter()
        for iter.Scan(&workerId) {
            //log.Printf("founded worker %d", workerId)
            if workerId == o.id {
                localWorkerModulus = counter
            }
            counter++
        }
        localWorkerTotal = counter
        iter.Close()
        o.mutex.Lock()
        if localWorkerTotal != o.total || localWorkerModulus != o.modulus {
            log.Printf("working cluster changed: %d/%d => %d/%d", 
                o.modulus, o.total, localWorkerModulus, localWorkerTotal)
            o.total = localWorkerTotal
            o.modulus = localWorkerModulus
        }
        o.mutex.Unlock()
        time.Sleep(time.Millisecond * time.Duration(o.Ttl * 1000 / 5))
    }
}

