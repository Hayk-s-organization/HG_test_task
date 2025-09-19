import { Database, Operations, Message } from "./Database";
import { Queue } from "./Queue";
import { range, sleep, getRandomIntInRange } from "./Util";
import { Worker } from "./Worker";

const ITEMS_NUMBER = getRandomIntInRange(3, 15);
const WORKERS_NUMBER = getRandomIntInRange(3, 6);

const ITEMS = range(ITEMS_NUMBER).map((item) => `item${item}`);

const applyToAll = (queue: Queue, operation: Operations, val: number): void => {
  for (const product of ITEMS) {
    queue.Enqueue(new Message(product, operation, val));
  }
};

const main = async () => {
  console.log(`Number of items:${ITEMS_NUMBER}`);
  console.log(`Number of workers:${WORKERS_NUMBER}`);

  const db = new Database();
  const queue = new Queue();

  applyToAll(queue, Operations.SET, 50);

  for (let i = 0; i < 10; i++) {
    applyToAll(queue, Operations.ADD, i);
  }

  range(WORKERS_NUMBER).forEach((i) => {
    const worker = new Worker(i, queue);
    worker.Work(db.set);
  });

  await sleep(10000);

  console.log("Queue size: ", queue.Size());
  console.log("DB state:\n", JSON.stringify(db.state(), null, 4));
};
main();
