"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.Queue = void 0;
var Queue = /** @class */ (function () {
    function Queue() {
        var _this = this;
        // Adds a new message to the queue
        this.Enqueue = function (message) {
            _this.messages.push(message);
        };
        // dequeues a message for a worker if available and locks it
        this.Dequeue = function () {
            for (var _i = 0, _a = _this.messages; _i < _a.length; _i++) {
                var message = _a[_i];
                // Skip if Database key is locked or the item is already being processed
                if (!_this.lockedKeys.has(message.key)) {
                    _this.lockedKeys.add(message.key); // Lock the DB key
                    return message;
                }
            }
            return undefined; // returns when there are no available messages left to process
        };
        // Confirms that the worker has finished processing of the message and removes it from the queue
        this.Confirm = function (workerId, messageId) {
            var index = _this.messages.findIndex(function (message) { return message.id === messageId; }); // finds the index of the message from messages array
            if (index !== -1) {
                var message = _this.messages[index];
                _this.lockedKeys.delete(message.key); // Unlock DB key so workers can process this DB key once again
                _this.messages.splice(index, 1); // Remove the processed message
            }
        };
        // Returns the number of messages left
        this.Size = function () {
            return _this.messages.length;
        };
        this.messages = [];
        this.lockedKeys = new Set();
    }
    return Queue;
}());
exports.Queue = Queue;
