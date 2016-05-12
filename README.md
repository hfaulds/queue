# queue (working title)

A multi-threaded in-memory single-server transactional queue.

A combination of a rust learning project and an experiment aimed at creating a transactional queue.

This project seeks to create a queue that is as simple to deploy and robust as redis but is restricted to just queues and provides transactions.

## transactions

Workers can crash or be explicitly terminated. Without some mechanism to store unfinished jobs data will be lost.

When using redis as the basis for a queue there are no transactions. One workaround is to use `RPOPLPUSH`, upon starting a job pop off the main queue and push to a worker specific queue. Once a job is finished remove the data from the worker specific queue. When a worker starts it checks its specific queue and runs any jobs that had not finshed. For more information see https://github.com/mperham/sidekiq/wiki/Reliability.

This works perfectly well but is conceptually complicated and relies on being able to easily identify each worker.

## commands

### QUIT

Self-explanatory

### PUSH queue_name, data

Push to queue. Will create queues if they don't exist.

### POP queue_name

Pop the oldest data off of the queue.

### BPOP queue_name

Blocking pop. Pop the oldest data off of the queue but block if there is no data.

### BEGIN

Start a transaction.

### COMMIT

Commit a transaction.

### ABORT

Abort a transaction.
