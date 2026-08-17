import { Decoder, Encoder } from "cbor-x";
import { SerializationError } from "../errors";
import type { Serializer } from "./serializer";

/** Wire-envelope tags (see flexiq-core BINDING_CONTRACT.md "Wire envelope"). */
const TAG_NATIVE = 0x00;
const TAG_CBOR = 0x02;

function tagged(body: Uint8Array): Uint8Array {
  const out = new Uint8Array(body.length + 1);
  out[0] = TAG_CBOR;
  out.set(body, 1);
  return out;
}

/**
 * CBOR serializer for cross-SDK payloads (RFC 8949), writing the `0x02`
 * wire-envelope tag. Use for tasks produced or consumed by another FlexiQ
 * SDK: unlike JSON, CBOR round-trips integers beyond `Number.MAX_SAFE_INTEGER`
 * (as `BigInt`), `Date` (tag 1), and binary data losslessly across languages.
 *
 * Call payloads use the cross-SDK call body `[args, kwargs]`; a producer with
 * keyword arguments surfaces them here as a trailing options object.
 */
export class CborSerializer implements Serializer {
  // Plain RFC 8949 structures only: cbor-x "records" and tag-259 maps are
  // extensions other SDKs cannot decode (tag 259 defaults ON once records are
  // off). `variableMapSize` emits minimal (canonical) map headers so output
  // matches other SDKs' encoders.
  // `useTag259ForMaps` is honored at runtime but missing from cbor-x's typings.
  private readonly encoder = new Encoder({
    useRecords: false,
    useTag259ForMaps: false,
    variableMapSize: true,
  } as ConstructorParameters<typeof Encoder>[0]);
  private readonly decoder = new Decoder({ useRecords: false, mapsAsObjects: true });

  serialize(value: unknown): Uint8Array {
    return tagged(this.encoder.encode(value ?? null));
  }

  deserialize(bytes: Uint8Array): unknown {
    return this.decoder.decode(this.body(bytes));
  }

  serializeCall(args: unknown[]): Uint8Array {
    return tagged(this.encoder.encode([args, {}]));
  }

  deserializeCall(bytes: Uint8Array): unknown[] {
    const decoded = this.decoder.decode(this.body(bytes));
    if (
      !Array.isArray(decoded) ||
      decoded.length !== 2 ||
      !Array.isArray(decoded[0]) ||
      decoded[1] === null ||
      typeof decoded[1] !== "object" ||
      Array.isArray(decoded[1])
    ) {
      throw new SerializationError("CBOR call payload is not the [args, kwargs] wire shape");
    }
    const [args, kwargs] = decoded as [unknown[], Record<string, unknown>];
    return Object.keys(kwargs).length > 0 ? [...args, kwargs] : args;
  }

  private body(bytes: Uint8Array): Uint8Array {
    if (bytes.length === 0) {
      throw new SerializationError("cannot deserialize an empty payload");
    }
    if (bytes[0] === TAG_CBOR) {
      return bytes.subarray(1);
    }
    if (bytes[0] === TAG_NATIVE) {
      throw new SerializationError(
        "payload is native-tagged (0x00): produced by a same-language-only serializer, not readable as CBOR wire format",
      );
    }
    throw new SerializationError(
      `payload is not CBOR wire format (tag 0x${bytes[0]?.toString(16).padStart(2, "0")}, expected 0x02)`,
    );
  }
}
