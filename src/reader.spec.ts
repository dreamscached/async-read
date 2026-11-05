import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import { createReader, StreamReader } from "./reader.js";
import { ReadableSource } from "./source.types.js";

/**
 * A simple event-emitting mock stream to simulate ReadableSource behavior.
 */
export class MockReadableSource implements ReadableSource {
	private listeners: { [eventName: string]: ((...args: any[]) => void)[] } = {};

	on(event: "data" | "end" | "error", listener: (...args: any[]) => void): this {
		if (!this.listeners[event]) this.listeners[event] = [];
		this.listeners[event].push(listener);
		return this;
	}

	emit(event: "data", chunk: Uint8Array): void;
	emit(event: "end"): void;
	emit(event: "error", err: unknown): void;
	emit(event: string, payload?: unknown): void {
		for (const fn of this.listeners[event] ?? []) {
			fn(payload);
		}
	}
}

describe("StreamReader", () => {
	let src: MockReadableSource;
	let reader: StreamReader<MockReadableSource>;

	beforeEach(() => {
		src = new MockReadableSource();
		reader = createReader(src);
	});

	afterEach(() => {
		vi.useRealTimers();
	});

	describe("read()", () => {
		it("leaves no timers pending after waiting for data", async () => {
			vi.useFakeTimers();
			const read = reader.read(2);
			src.emit("data", Uint8Array.from([1, 2]));
			await vi.advanceTimersByTimeAsync(1);
			await expect(read).resolves.toEqual(Uint8Array.from([1, 2]));
			expect(vi.getTimerCount()).toBe(0);
		});

		it("reads buffered data from a single chunk", async () => {
			src.emit("data", Uint8Array.from([1, 2, 3]));
			const result = await reader.read(3);
			expect(result).toEqual(Uint8Array.from([1, 2, 3]));
		});

		it("reads incoming data from a single chunk", async () => {
			setTimeout(() => src.emit("data", Uint8Array.from([1, 2, 3])), 10);
			const result = await reader.read(3);
			expect(result).toEqual(Uint8Array.from([1, 2, 3]));
		});

		it("reads data across multiple chunks", async () => {
			src.emit("data", Uint8Array.from([1]));
			setTimeout(() => src.emit("data", Uint8Array.from([2, 3])), 10);
			const result = await reader.read(3);
			expect(result).toEqual(Uint8Array.from([1, 2, 3]));
		});

		it("buffers and returns excess data", async () => {
			src.emit("data", Uint8Array.from([10, 20, 30, 40]));
			const first = await reader.read(2);
			const second = await reader.read(2);
			expect(first).toEqual(Uint8Array.from([10, 20]));
			expect(second).toEqual(Uint8Array.from([30, 40]));
		});

		it("allows multiple concurrent reads to resolve in correct order", async () => {
			const read1 = reader.read(1);
			const read2 = reader.read(1);
			src.emit("data", Uint8Array.from([42]));
			src.emit("data", Uint8Array.from([99]));
			await expect(read1).resolves.toEqual(Uint8Array.from([42]));
			await expect(read2).resolves.toEqual(Uint8Array.from([99]));
		});

		it("throws on end before any data while reading", async () => {
			src.emit("end");
			await expect(reader.read(1)).rejects.toThrow("end of stream");
		});

		it("can consume buffered data after end and then errors", async () => {
			src.emit("data", Uint8Array.from([7, 8, 9]));
			src.emit("end");
			// still buffered
			expect(reader.buffered()).toBe(3);
			const out = await reader.read(3);
			expect(out).toEqual(Uint8Array.from([7, 8, 9]));
			// now error on further read
			await expect(reader.read(1)).rejects.toThrow("end of stream");
		});

		it("throws on non-integer size", async () => {
			await expect(reader.read(2.5)).rejects.toThrow("size must be an integer number");
		});

		it("throws on non-positive size", async () => {
			await expect(reader.read(0)).rejects.toThrow("size must be a positive number");
			await expect(reader.read(-1)).rejects.toThrow("size must be a positive number");
		});

		it("throws on NaN or infinite size", async () => {
			await expect(reader.read(NaN)).rejects.toThrow("size must be an integer number");
			await expect(reader.read(Infinity)).rejects.toThrow("size must be an integer number");
		});

		it("throws on premature end of stream", async () => {
			src.emit("data", Uint8Array.from([1]));
			setTimeout(() => src.emit("end"), 5);
			await expect(reader.read(2)).rejects.toThrow("end of stream");
		});

		it("throws on emitted error", async () => {
			setTimeout(() => src.emit("error", new Error("fail")), 5);
			await expect(reader.read(1)).rejects.toThrow("fail");
		});

		it("rejects pending read when error arrives mid-read", async () => {
			src.emit("data", Uint8Array.from([1]));
			const p = reader.read(2);
			setTimeout(() => src.emit("error", new Error("")), 5);
			await expect(p).rejects.toThrow("");
		});

		it("can read exact final chunk before end", async () => {
			src.emit("data", Uint8Array.from([1, 2]));
			setTimeout(() => {
				src.emit("data", Uint8Array.from([3]));
				src.emit("end");
			}, 5);
			const result = await reader.read(3);
			expect(result).toEqual(Uint8Array.from([1, 2, 3]));
		});
	});

	describe("buffered()", () => {
		it("returns 0 when no data is written", () => {
			expect(reader.buffered()).toBe(0);
		});

		it("reflects total bytes after a single emit", () => {
			src.emit("data", Uint8Array.from([1, 2, 3]));
			expect(reader.buffered()).toBe(3);
		});

		it("reflects remaining bytes after partial read", async () => {
			src.emit("data", Uint8Array.from([1, 2, 3]));
			await reader.read(2);
			expect(reader.buffered()).toBe(1);
		});

		it("is 0 after full consumption", async () => {
			src.emit("data", Uint8Array.from([100, 200]));
			await reader.read(2);
			expect(reader.buffered()).toBe(0);
		});

		it("aggregates size across multiple chunks", () => {
			src.emit("data", Uint8Array.from([1, 2]));
			src.emit("data", Uint8Array.from([3, 4, 5]));
			expect(reader.buffered()).toBe(5);
		});

		it("buffered remains after end until consumed", () => {
			src.emit("data", Uint8Array.from([5, 6]));
			src.emit("end");
			expect(reader.buffered()).toBe(2);
		});
	});
});
