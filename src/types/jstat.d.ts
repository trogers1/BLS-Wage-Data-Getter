declare module "jstat" {
  // Vector functions
  export function mean(arr: number[]): number;
  export function stdev(arr: number[], sample?: boolean): number;
  export function min(arr: number[]): number;
  export function max(arr: number[]): number;
  export function quartiles(arr: number[]): [number, number, number];

  // Correlation functions
  export function corrcoeff(arr1: number[], arr2: number[]): number;
  export function spearmancoeff(arr1: number[], arr2: number[]): number;

  // Distributions
  export const studentt: {
    cdf(x: number, df: number): number;
    inv(p: number, df: number): number;
    pdf(x: number, df: number): number;
  };

  // Default export
  const jStat: {
    mean(arr: number[]): number;
    stdev(arr: number[], sample?: boolean): number;
    min(arr: number[]): number;
    max(arr: number[]): number;
    quartiles(arr: number[]): [number, number, number];
    corrcoeff(arr1: number[], arr2: number[]): number;
    spearmancoeff(arr1: number[], arr2: number[]): number;
    studentt: {
      cdf(x: number, df: number): number;
      inv(p: number, df: number): number;
      pdf(x: number, df: number): number;
    };
  };

  export default jStat;
}
