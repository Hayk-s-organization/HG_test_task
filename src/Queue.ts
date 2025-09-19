import { Message } from "./Database";

/*Initial issue was related to the case tham message with the same key id we should process sequenitally
I have used sqs fifo queue approach with message group id
all messages within group are processed sequentially
group id is set to message.key

I do understand that provided interface is more suitable for kafka and kafka consumers group style,
but it seems for me that with the consumer group would be better to have additional interface for consumer to register itself
and have additional logic to track that consumer is alive
Seems that sqs approach is easier to implement as a test case

In the implementation I tried to do all the operations in O(1)
Messages are sharded by group id, messages within the group processed sequentially
if not message within the group is not processsed, any available worker could process it

Interfaces changes:
With current implementation we do not need workedId
and for confirmation we should send group Id key, not message id

If needed I can provide implementaion in kafka approach
*/
export class Queue {
  //messages grouped per group id
  //Map and set are ordered in es6 sepcification, we can use this and avoid using array with shift
  //although array shift seems to be optimized internally and do not take O(n), ordered set for storying message shoule take exactly O(1)
  private groupedMessages: Map<string, Set<Message>>;
  //available to process group ids
  private readyToProcess: Set<string>;
  private size: number;
  //message propery name used to shard the messages
  private messageGroupId: string;

  constructor() {
    this.messageGroupId = "key";
    this.groupedMessages = new Map();
    this.readyToProcess = new Set();
    this.size = 0;
  }

  Enqueue = (message: Message) => {
    const groupId = message[this.messageGroupId];
    const messages = this.groupedMessages.get(groupId) || new Set();
    if (!messages.size) {
      this.readyToProcess.add(groupId);
    }
    messages.add(message);
    this.groupedMessages.set(groupId, messages);
    this.size++;
  };

  Dequeue = (): Message | undefined => {
    if (!this.readyToProcess.size) return undefined;

    const groupId = this.readyToProcess.keys().next().value!;
    const messages = this.groupedMessages.get(groupId);
    //this if check is not required, beacause we did readyToProcess.size
    //but I have left it to make typescript happy
    if (!messages) return undefined;

    const messageToProcess = messages.values().next().value!;
    messages.delete(messageToProcess);
    this.readyToProcess.delete(groupId);
    this.size--;
    return messageToProcess;
  };

  //should provide messageGroupId as parameter
  Confirm = (workerId: number, messageId: string) => {
    //idealy we should provide messageGroupId=message.key to the method
    //with current interface we need to get group id via split
    const groupId = messageId.split(":")[0];

    if (this.groupedMessages.get(groupId)?.size)
      this.readyToProcess.add(groupId);
  };

  Size = () => {
    return this.size;
  };
}
