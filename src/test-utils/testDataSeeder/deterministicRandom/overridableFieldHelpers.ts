function isFunction<T>(field: unknown): field is (seed: string) => T {
  return typeof field === "function";
}

export function resolveFieldThatMayBeAFunction<T>({
  field,
  randomSeed,
}: {
  field: T | ((seed: string) => T);
  randomSeed: string;
}): T {
  return isFunction<T>(field) ? field(randomSeed) : field;
}

export function normalizeFuncPropertiesToValues<
  T extends Record<string, unknown>,
>({
  obj,
  randomSeed,
}: {
  obj: {
    [K in keyof T]?: T[K] | ((seed: string) => T[K]);
  };
  randomSeed: string;
}): Partial<T> {
  const result: Partial<T> = {};
  for (const key in obj) {
    if (Object.prototype.hasOwnProperty.call(obj, key)) {
      result[key] = resolveFieldThatMayBeAFunction({
        field: obj[key],
        randomSeed,
      }) as T[Extract<keyof T, string>];
    }
  }
  return result;
}
