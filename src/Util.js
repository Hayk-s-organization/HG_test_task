"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.sleep = sleep;
exports.getRandomInt = getRandomInt;
exports.getRandomIntInRange = getRandomIntInRange;
exports.range = range;
function sleep(ms) {
    return new Promise(function (resolve) { return setTimeout(resolve, ms); });
}
function getRandomInt(max) {
    return Math.floor(Math.random() * max);
}
function getRandomIntInRange(from, to) {
    return Math.floor(Math.random() * (to - from)) + from;
}
function range(n) {
    return Array.from({ length: n }, function (_, index) { return index; });
}
