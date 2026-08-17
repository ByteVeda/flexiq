package org.byteveda.flexiq.internal;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import org.byteveda.flexiq.FlexiQException;

/**
 * Project Panama (FFM) fast path for the hot byte ops, used instead of JNI on
 * JDK 22+. Lives only in the {@code META-INF/versions/22} overlay of the jar, so
 * it is loaded only where {@code java.lang.foreign} is stable; {@link
 * NativeTransport#create} resolves it reflectively and falls back to JNI when it
 * (or its native symbols) are absent.
 *
 * <p>It calls the {@code flexiq_ffi_*} C-ABI exports in the same cdylib the JNI
 * surface uses — the library is already in the process via {@link
 * NativeLoader#load()}, so symbols resolve through {@link
 * SymbolLookup#loaderLookup()}. Each call confines its native arguments to a
 * short-lived {@link Arena}; results are copied out and freed via {@code
 * flexiq_ffi_free}. The calls perform blocking storage I/O, so they are ordinary
 * (non-{@code critical}) downcalls — never hold the heap for the JVM.
 */
public final class FfmTransport implements NativeTransport {

    /**
     * Little-endian framing matching the Rust side's {@code u32::to_le_bytes}.
     * Unaligned because the length prefixes sit at arbitrary offsets after each
     * variable-length payload in the packed frame.
     */
    private static final ValueLayout.OfInt FRAME_INT =
            ValueLayout.JAVA_INT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);

    // Wire status codes from the Rust C ABI; anything but these two carries an error payload.
    private static final int STATUS_OK = 0;
    private static final int STATUS_ABSENT = 2;

    private static final Linker LINKER = Linker.nativeLinker();
    private static final MethodHandle ENQUEUE;
    private static final MethodHandle ENQUEUE_MANY;
    private static final MethodHandle GET_RESULT;
    private static final MethodHandle FREE;

    static {
        NativeLoader.load(); // ensure the cdylib is loaded (System.load) before lookup
        SymbolLookup lookup = SymbolLookup.loaderLookup();
        FunctionDescriptor producer = FunctionDescriptor.of(
                ValueLayout.JAVA_INT, // status
                ValueLayout.JAVA_LONG, // handle
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG, // task ptr,len
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG, // payload/frame ptr,len
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG, // options ptr,len
                ValueLayout.ADDRESS,
                ValueLayout.ADDRESS); // out_data, out_len
        ENQUEUE = downcall(lookup, "flexiq_ffi_enqueue", producer);
        ENQUEUE_MANY = downcall(lookup, "flexiq_ffi_enqueue_many", producer);
        GET_RESULT = downcall(
                lookup,
                "flexiq_ffi_get_result",
                FunctionDescriptor.of(
                        ValueLayout.JAVA_INT,
                        ValueLayout.JAVA_LONG,
                        ValueLayout.ADDRESS,
                        ValueLayout.JAVA_LONG, // job-id ptr,len
                        ValueLayout.ADDRESS,
                        ValueLayout.ADDRESS)); // out_data, out_len
        FREE = downcall(
                lookup, "flexiq_ffi_free", FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_LONG));
    }

    private final long handle;

    private FfmTransport(long handle) {
        this.handle = handle;
    }

    /** Factory invoked reflectively by {@link NativeTransport#create} on JDK 22+. */
    public static NativeTransport create(long handle) {
        return new FfmTransport(handle);
    }

    @Override
    public String enqueue(String taskName, byte[] payload, String optionsJson) {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment task = utf8(arena, taskName);
            MemorySegment data = bytes(arena, payload);
            MemorySegment options = utf8(arena, optionsJson);
            MemorySegment outData = arena.allocate(ValueLayout.ADDRESS);
            MemorySegment outLen = arena.allocate(ValueLayout.JAVA_LONG);
            int status = (int) ENQUEUE.invokeExact(
                    handle, task, task.byteSize(), data, data.byteSize(), options, options.byteSize(), outData, outLen);
            byte[] out = take(outData, outLen);
            if (status == STATUS_OK) {
                return new String(out, StandardCharsets.UTF_8);
            }
            throw error(out);
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    @Override
    public String[] enqueueMany(String taskName, byte[][] payloads, String optionsJson) {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment task = utf8(arena, taskName);
            MemorySegment framed = frame(arena, payloads);
            MemorySegment options = utf8(arena, optionsJson);
            MemorySegment outData = arena.allocate(ValueLayout.ADDRESS);
            MemorySegment outLen = arena.allocate(ValueLayout.JAVA_LONG);
            int status = (int) ENQUEUE_MANY.invokeExact(
                    handle,
                    task,
                    task.byteSize(),
                    framed,
                    framed.byteSize(),
                    options,
                    options.byteSize(),
                    outData,
                    outLen);
            byte[] out = take(outData, outLen);
            if (status == STATUS_OK) {
                return unframe(out);
            }
            throw error(out);
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    @Override
    public byte[] getResult(String jobId) {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment id = utf8(arena, jobId);
            MemorySegment outData = arena.allocate(ValueLayout.ADDRESS);
            MemorySegment outLen = arena.allocate(ValueLayout.JAVA_LONG);
            int status = (int) GET_RESULT.invokeExact(handle, id, id.byteSize(), outData, outLen);
            if (status == STATUS_ABSENT) {
                return null;
            }
            byte[] out = take(outData, outLen);
            if (status == STATUS_OK) {
                return out;
            }
            throw error(out);
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    private static MethodHandle downcall(SymbolLookup lookup, String symbol, FunctionDescriptor descriptor) {
        MemorySegment address =
                lookup.find(symbol).orElseThrow(() -> new UnsatisfiedLinkError("missing C-ABI symbol " + symbol));
        return LINKER.downcallHandle(address, descriptor);
    }

    /** Allocate {@code bytes} off-heap for the call; an empty array maps to a null pointer. */
    private static MemorySegment bytes(Arena arena, byte[] value) {
        if (value == null || value.length == 0) {
            return MemorySegment.NULL;
        }
        MemorySegment segment = arena.allocate(value.length);
        MemorySegment.copy(value, 0, segment, ValueLayout.JAVA_BYTE, 0, value.length);
        return segment;
    }

    private static MemorySegment utf8(Arena arena, String value) {
        return bytes(arena, value.getBytes(StandardCharsets.UTF_8));
    }

    /** Encode {@code [count][len][bytes]...} (LE) for the batch payloads. */
    private static MemorySegment frame(Arena arena, byte[][] items) {
        long size = Integer.BYTES;
        for (byte[] item : items) {
            size += Integer.BYTES + item.length;
        }
        MemorySegment segment = arena.allocate(size);
        long offset = 0;
        segment.set(FRAME_INT, offset, items.length);
        offset += Integer.BYTES;
        for (byte[] item : items) {
            segment.set(FRAME_INT, offset, item.length);
            offset += Integer.BYTES;
            MemorySegment.copy(item, 0, segment, ValueLayout.JAVA_BYTE, offset, item.length);
            offset += item.length;
        }
        return segment;
    }

    /** Decode a {@code [count][len][bytes]...} (LE) frame of job ids. */
    private static String[] unframe(byte[] buffer) {
        ByteBuffer reader = ByteBuffer.wrap(buffer).order(ByteOrder.LITTLE_ENDIAN);
        String[] ids = new String[reader.getInt()];
        for (int i = 0; i < ids.length; i++) {
            byte[] id = new byte[reader.getInt()];
            reader.get(id);
            ids[i] = new String(id, StandardCharsets.UTF_8);
        }
        return ids;
    }

    /** Copy out the bytes the native side allocated, then free them. */
    private static byte[] take(MemorySegment outData, MemorySegment outLen) {
        MemorySegment pointer = outData.get(ValueLayout.ADDRESS, 0);
        long length = outLen.get(ValueLayout.JAVA_LONG, 0);
        if (pointer.address() == 0 || length == 0) {
            return new byte[0];
        }
        // Free in finally so a failed copy never leaks the Rust allocation.
        try {
            return pointer.reinterpret(length).toArray(ValueLayout.JAVA_BYTE);
        } finally {
            free(pointer, length);
        }
    }

    private static void free(MemorySegment pointer, long length) {
        try {
            FREE.invokeExact(pointer, length);
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    private static FlexiQException error(byte[] message) {
        return new FlexiQException(new String(message, StandardCharsets.UTF_8));
    }

    private static RuntimeException rethrow(Throwable t) {
        if (t instanceof RuntimeException runtime) {
            return runtime;
        }
        if (t instanceof Error error) {
            throw error;
        }
        return new FlexiQException("FFM transport failure: " + t.getMessage(), t);
    }
}
