/**
 * Interface for an EventEmitter-compatible readable data source that emits typed events.
 */
export interface ReadableSource {
	on(event: "data", listener: (chunk: Uint8Array) => void): this;
	on(event: "end", listener: () => void): this;
	on(event: "error", listener: (err: unknown) => void): this;
}
