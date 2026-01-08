/**
 * Simple deterministic hash function for string to number conversion
 * Uses djb2 algorithm for good distribution
 */
export function hashString(str: string): number {
  let hash = 5381; // DJB2 'Magic Number': https://stackoverflow.com/a/13809282
  for (let i = 0; i < str.length; i++) {
    hash = (hash << 5) + hash + str.charCodeAt(i);
    hash = hash | 0; // Convert to 32-bit integer
  }
  return hash;
}
