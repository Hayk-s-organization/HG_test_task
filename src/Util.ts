export function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
export function getRandomInt(max: number): number {
  return Math.floor(Math.random() * max);
}

export function getRandomIntInRange(from: number, to: number): number {
  return Math.floor(Math.random() * (to - from)) + from;
}

export function range(n: number): number[] {
  return Array.from({ length: n }, (_, index) => index);
}
