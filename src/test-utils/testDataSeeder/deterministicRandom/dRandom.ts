import { hashString } from "./hashing.ts";

export function oneOf<T>({ seed, options }: { seed: string; options: T[] }): T {
  const hash = hashString(seed);
  const index = Math.abs(hash) % options.length;
  return options[index];
}

export function randomInt({
  seed,
  min,
  max,
}: {
  seed: string;
  min: number;
  max: number;
}): number {
  const hash = hashString(seed);
  const range = max - min + 1; // +1 to include max in the range
  const value = Math.abs(hash) % range;
  return min + value;
}

export function shuffle<T>({
  randomSeed,
  options,
}: {
  randomSeed: string;
  options: T[];
}): T[] {
  // Create a copy to avoid mutating the original
  const shuffled = [...options];

  // Sort by hash of seed concatenated with element (coerced to string)
  shuffled.sort((a, b) => {
    const hashA = hashString(`${randomSeed}-${a}`);
    const hashB = hashString(`${randomSeed}-${b}`);
    return hashA - hashB;
  });

  return shuffled;
}
