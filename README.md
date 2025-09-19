# senior-be-developer-task

## Introduction

Hello! If you are viewing this repository you are probably a candidate for HyperGuest senior backend developer, congrats!

Before we begin, a few important notes please!

- Any AI tool is forbidden during this task. We already know Github copilot is a great tool, we don't need to test it :)
- This task is designated to test your problem-solving skills, still, code quality do matters! submit a code you would like to read as well
- At your sumption please include the next things :
  - your implementation of `Queues.ts`. Any other files are not allowed to be changed during this test.
  - write a clear explanation of your implementation.
  - suggest improvements! the code in this repository is not perfect, what would you do differently?
- Do not PR to this repo! if you would, all of your opponents would copy your answers ;)

Good luck!

## Task description

The code in this repo is not working properly.

The script `main.ts` is trying to write "a lot" of data into a simulation of a key-value database ( implemented in `Datable.ts`). As Databases usually do - our database has small latency - between 0 to 100 ms per operation.
In order to allow faster process of the messages our `main.ts` script puts all the operations in a queue (see the `Queue.ts` file). Than it launches a random number (between 3 to 6) of "workers" (see `Worker.ts`), that works asynchronously in parallel, reading messages from the queue and commit the operation to the "Database". Each worker should "confirm" to the queue that the message was proceeded, so the message would be deleted.
The script wait for 10 seconds (enough time for all the workers to complete the work), prints the state of hte queue and the DB state, and exits.

However! <br />
the results are wrong :( <br />
We set all the initial values to 50, and than we add all numbers between 1 to 9 for each item. therefore the correct results so the correct output should be:

```bash
# > ts-node main.ts
Number of items:3
Number of workers:5
Queue size:  0
DB state:
 {
    "item2": 95,
    "item0": 95,
    "item1": 95
}
```

While running the script would look like this (different values would be shown on each run):

```bash
# > ts-node main.ts
Number of items:3
Number of workers:5
Queue size:  0
DB state:
 {
    "item2": 74,
    "item0": 74,
    "item1": 15
}
```

Please assist our dev team to implement a valid version of `Queue.ts`!

_note_: Any implementation that would effectively not allow any parallel work between the workers would be rejected.
