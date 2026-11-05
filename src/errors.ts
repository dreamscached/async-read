/**
 * Thrown by `StreamReader.read()` when the stream ends before the requested number of bytes arrives.
 */
export class EndOfStreamError extends Error {
	/**
	 * The bytes read before the stream ended. Empty if the stream ended cleanly on a read
	 * boundary, non-empty if the data was truncated. These bytes are removed from the reader's buffer.
	 */
	public readonly partial: Uint8Array;

	/**
	 * @param partial - The bytes read before the stream ended.
	 */
	public constructor(partial: Uint8Array) {
		super("end of stream");
		this.name = "EndOfStreamError";
		this.partial = partial;
	}
}
