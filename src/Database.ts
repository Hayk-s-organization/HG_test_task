import { getRandomInt, sleep } from "./Util";

export enum Operations {
  ADD,
  SUBTRACT,
  SET,
}

// Type alias for the data structure
type DatabaseData = { [key: string]: number };

export class Database {
  private readonly data: DatabaseData;

  constructor() {
    this.data = {};
  }

  set = async (message: Message): Promise<void> => {
    const current = this.data[message.key] || 0;
    let result: number;
    switch (message.operation) {
      case Operations.ADD:
        result = current + message.val;
        break;
      case Operations.SUBTRACT:
        result = current - message.val;
        break;
      case Operations.SET:
        result = message.val;
        break;
      default:
        throw Error(`Invalid operation ${message.operation}`);
    }
    const randomDelay = getRandomInt(100);
    await sleep(randomDelay);
    this.data[message.key] = result;
  };

  get = (key: string): number => this.data[key];

  // Provide a type for the return value
  state = (): DatabaseData => this.data;
}

export class Message {
  public key: string;
  public operation: Operations;
  public val: number;
  public id: string;

  constructor(key: string, operation: Operations, val: number) {
    this.key = key;
    this.operation = operation;
    this.val = val;
    this.id = this.generateId(key, operation, val);
  }

  // Simplify the ID generation
  private generateId(key: string, operation: Operations, val: number): string {
    return `${key}:${val}:${operation}:${Date.now()}:${Math.random().toString(36).substr(2, 9)}`;
  }
}
