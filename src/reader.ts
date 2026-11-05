import { type PromiseSettleFunctions } from "./promise.types.js";
import { type ReadableSource } from "./source.types.js";

/**
 * Creates a new StreamReader instance for the given readable source.
 *
 * @param source - A readable stream-like source emitting 'data', 'end', and 'error' events.
 * @returns A StreamReader instance that provides buffered asynchronous reads.
 */
export function createReader<T extends ReadableSource>(source: T) {
	return new StreamReader(source);
}

/**
 * StreamReader provides buffered, asynchronous byte-level reading from a
 * readable event-based stream (e.g., Node.js streams, custom event emitters).
 *
 * It collects `Uint8Array` chunks from a `ReadableSource` and enables consumers
 * to await exact byte sizes reliably.
 */
export class StreamReader<T extends ReadableSource> {
	private _readBuffer = new Uint8Array(0);
	private _endOfStream = false;
	private _error: unknown = null;
	private _callbacks: PromiseSettleFunctions<boolean>[] = [];

	/**
	 * Initializes a new StreamReader and subscribes to the source stream's events.
	 *
	 * @param source - A readable stream emitting 'data', 'end', and 'error' events.
	 */
	public constructor(source: T) {
		source.on("data", (data) => {
			this._writeToBuffer(data);
			this._resolveReadPromise(true);
		});

		source.on("end", () => {
			this._endOfStream = true;
			this._resolveReadPromise(false);
		});

		source.on("error", (err) => {
			this._error = err;
			this._endOfStream = true;
			this._rejectReadPromise(err);
		});
	}

	/**
	 * Reads exactly `size` bytes from the internal buffer.
	 *
	 * If not enough data is currently available, this method waits until enough
	 * data is received or throws if the stream ends or an error occurs first.
	 *
	 * @param size - The number of bytes to read (must be a positive integer).
	 * @returns A Uint8Array of the requested byte length.
	 * @throws Error if `size` is invalid, or if the stream ends/errors before enough data arrives.
	 */
	public async read(size: number): Promise<Uint8Array> {
		if (!Number.isInteger(size)) throw new Error("size must be an integer number");
		if (size <= 0) throw new Error("size must be a positive number");

		while (this._readBuffer.length < size) {
			const hasMore = await this._awaitBuffer();
			if (!hasMore) throw new Error("end of stream");
			await new Promise((resolve) => setInterval(resolve, 0)); // yield
		}

		const result = this._readBuffer.subarray(0, size);
		this._readBuffer = this._readBuffer.subarray(size);
		return result;
	}

	/**
	 * Returns the number of bytes currently available in the buffer.
	 *
	 * @returns The length of the internal byte buffer.
	 */
	public buffered(): number {
		return this._readBuffer.length;
	}

	/**
	 * Appends new data to the internal read buffer.
	 *
	 * @param data - Incoming data to append.
	 */
	private _writeToBuffer(data: Uint8Array): void {
		const newBuffer = new Uint8Array(this._readBuffer.length + data.length);
		newBuffer.set(this._readBuffer);
		newBuffer.set(data, this._readBuffer.length);
		this._readBuffer = newBuffer;
	}

	/**
	 * Resolves the next pending read promise with a flag indicating
	 * whether more data may arrive.
	 *
	 * @param hasMore - `true` if more data may arrive; `false` if the stream has ended.
	 */
	private _resolveReadPromise(hasMore: boolean): void {
		const cb = this._callbacks.shift();
		if (cb !== undefined) {
			const [resolve] = cb;
			resolve(hasMore);
		}
	}

	/**
	 * Rejects the next pending read promise with the provided error.
	 *
	 * @param err - The error that caused the stream to fail.
	 */
	private _rejectReadPromise(err: unknown): void {
		const cb = this._callbacks.shift();
		if (cb !== undefined) {
			const [, reject] = cb;
			reject(err);
		}
	}

	/**
	 * Waits for new data to be buffered, the stream to end, or an error to occur.
	 *
	 * @returns A promise resolving to `true` if data may arrive, `false` if the stream ended.
	 * @throws If the stream emits an error.
	 */
	private async _awaitBuffer(): Promise<boolean> {
		if (this._error) throw this._error;
		if (this._endOfStream) return false;
		if (this._readBuffer.length > 0) return true;

		return new Promise((resolve, reject) => {
			this._callbacks.push([resolve, reject]);
		});
	}
}
