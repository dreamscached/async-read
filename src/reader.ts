import { EndOfStreamError } from "./errors.js";
import { type PromiseSettleFunctions } from "./promise.types.js";
import { type ReadableSource } from "./source.types.js";

/**
 * A read waiting in the queue for enough data to be settled.
 */
interface PendingRead {
	size: number;
	exact: boolean;
	settle: PromiseSettleFunctions<Uint8Array>;
}

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
 * to await exact byte sizes reliably. Concurrent reads settle in the order they were made.
 */
export class StreamReader<T extends ReadableSource> {
	private _readBuffer = new Uint8Array(0);
	private _endOfStream = false;
	private _error: unknown = null;
	private _pending: PendingRead[] = [];

	/**
	 * Initializes a new StreamReader and subscribes to the source stream's events.
	 *
	 * @param source - A readable stream emitting 'data', 'end', and 'error' events.
	 */
	public constructor(source: T) {
		source.on("data", (data) => {
			this._writeToBuffer(data);
			this._settlePending();
		});

		source.on("end", () => {
			this._endOfStream = true;
			this._settlePending();
		});

		source.on("error", (err) => {
			this._error = err;
			this._endOfStream = true;
			this._settlePending();
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
	 * @throws Error if `size` is invalid, or if the stream errors before enough data arrives.
	 * @throws EndOfStreamError if the stream ends before enough data arrives.
	 */
	public async read(size: number): Promise<Uint8Array> {
		return this._enqueue(size, true);
	}

	/**
	 * Reads at most `size` bytes, waiting only until at least one byte is available.
	 *
	 * @param size - The maximum number of bytes to read (must be a positive integer).
	 * @returns A Uint8Array of 1 to `size` bytes, or an empty one if the stream has ended.
	 * @throws Error if `size` is invalid, or if the stream errors before any data arrives.
	 */
	public async readUpTo(size: number): Promise<Uint8Array> {
		return this._enqueue(size, false);
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
	 * Removes up to `size` bytes from the front of the internal read buffer.
	 *
	 * @param size - The number of bytes to remove.
	 * @returns The removed bytes.
	 */
	private _takeFromBuffer(size: number): Uint8Array {
		const result = this._readBuffer.subarray(0, size);
		this._readBuffer = this._readBuffer.subarray(size);
		return result;
	}

	/**
	 * Validates a read request and puts it at the back of the queue.
	 *
	 * @param size - The requested number of bytes.
	 * @param exact - `true` to require exactly `size` bytes; `false` to accept fewer.
	 * @returns A promise settled once the read reaches the front of the queue and can complete.
	 */
	private async _enqueue(size: number, exact: boolean): Promise<Uint8Array> {
		if (!Number.isInteger(size)) throw new Error("size must be an integer number");
		if (size <= 0) throw new Error("size must be a positive number");

		return new Promise((resolve, reject) => {
			this._pending.push({ size, exact, settle: [resolve, reject] });
			this._settlePending();
		});
	}

	/**
	 * Settles queued reads from the front, stopping at the first one that has to keep waiting.
	 */
	private _settlePending(): void {
		while (this._pending[0] !== undefined && this._trySettle(this._pending[0])) {
			this._pending.shift();
		}
	}

	/**
	 * Settles a read if the buffered data and the stream state allow it.
	 *
	 * Buffered data is served before a stream error or end of stream is reported.
	 *
	 * @param read - The read to settle.
	 * @returns `true` if the read was settled; `false` if it has to keep waiting.
	 */
	private _trySettle({ size, exact, settle: [resolve, reject] }: PendingRead): boolean {
		const available = this._readBuffer.length;

		if (available >= size || (!exact && available > 0)) {
			resolve(this._takeFromBuffer(size));
		} else if (!this._endOfStream) {
			return false;
		} else if (this._error !== null) {
			reject(this._error);
		} else if (exact) {
			reject(new EndOfStreamError(this._takeFromBuffer(available)));
		} else {
			resolve(new Uint8Array(0));
		}

		return true;
	}
}
