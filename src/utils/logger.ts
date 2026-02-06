export const log: typeof console.log = (args) =>
  process.env.DEBUG === "true" && console.log(...args);
