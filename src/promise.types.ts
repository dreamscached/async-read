/**
 * A function that resolves a Promise with a given value.
 *
 * This matches the `resolve` function passed to the `new Promise<T>(resolve, reject)` constructor.
 *
 * @template T - The type of the resolved value.
 */
export interface PromiseResolveFunction<T> {
	(value: T): unknown;
}

/**
 * A function that rejects a Promise with an error.
 *
 * This matches the `reject` function passed to the `new Promise<T>(resolve, reject)` constructor.
 */
export interface PromiseRejectFunction {
	(error: unknown): unknown;
}

/**
 * A pair of resolve and reject functions from a Promise constructor.
 *
 * Represents both `resolve` and `reject` as a tuple, typically used to manually control Promise settlement.
 *
 * @template T - The type the Promise resolves with.
 */
export type PromiseSettleFunctions<T> = [PromiseResolveFunction<T>, PromiseRejectFunction];
