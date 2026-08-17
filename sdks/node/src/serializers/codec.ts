import { JsonSerializer } from "./json";
import type { Serializer } from "./serializer";

/**
 * Reversible byte transform layered over a serializer — compression,
 * encryption, signing. `encode` runs on the producer after serialization;
 * `decode` reverses it on the worker before deserialization.
 *
 * Codecs compose: a chain encodes in list order and decodes in reverse, so
 * `[gzip, hmac]` verifies integrity *before* decompressing. Wire formats are
 * part of the cross-SDK contract, so codec-framed payloads decode from any
 * FlexiQ SDK.
 */
export interface PayloadCodec {
  /** Transform serialized bytes on the producer (compress, encrypt, sign). */
  encode(data: Uint8Array): Uint8Array;
  /** Reverse the transform on the worker. */
  decode(data: Uint8Array): Uint8Array;
}

/**
 * Serializer decorator applying a codec chain around a delegate. `serialize`
 * runs the delegate then encodes codecs in list order; `deserialize` decodes
 * in reverse order then delegates. Used internally when `QueueOptions.codec`
 * is set — the chain then covers every payload and result flowing through the
 * queue serializer.
 */
export class CodecSerializer implements Serializer {
  private readonly delegate: Serializer;
  private readonly codecs: readonly PayloadCodec[];

  /** Present only when the delegate is call-aware, so callers can detect it. */
  readonly serializeCall?: (args: unknown[]) => Uint8Array;
  readonly deserializeCall?: (bytes: Uint8Array) => unknown[];

  constructor(delegate: Serializer = new JsonSerializer(), codecs: readonly PayloadCodec[] = []) {
    this.delegate = delegate;
    this.codecs = [...codecs];
    const serializeCall = delegate.serializeCall?.bind(delegate);
    if (serializeCall) {
      this.serializeCall = (args) => this.encode(serializeCall(args));
    }
    const deserializeCall = delegate.deserializeCall?.bind(delegate);
    if (deserializeCall) {
      this.deserializeCall = (bytes) => deserializeCall(this.decode(bytes));
    }
  }

  serialize(value: unknown): Uint8Array {
    return this.encode(this.delegate.serialize(value));
  }

  deserialize(bytes: Uint8Array): unknown {
    return this.delegate.deserialize(this.decode(bytes));
  }

  private encode(data: Uint8Array): Uint8Array {
    let out = data;
    for (const codec of this.codecs) {
      out = codec.encode(out);
    }
    return out;
  }

  private decode(bytes: Uint8Array): Uint8Array {
    let out = bytes;
    for (const codec of [...this.codecs].reverse()) {
      out = codec.decode(out);
    }
    return out;
  }
}
