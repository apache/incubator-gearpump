/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gearpump.streaming.refactor.coder;

import com.google.common.io.BaseEncoding;

import java.io.*;
import java.lang.ref.SoftReference;

/**
 * Utilities for working with Coders.
 */
public final class CoderUtils {
    private CoderUtils() {
    }  // Non-instantiable

    private static ThreadLocal<SoftReference<ExposedByteArrayOutputStream>>
            threadLocalOutputStream = new ThreadLocal<>();

    /**
     * If true, a call to {@code encodeToByteArray} is already on the call stack.
     */
    private static ThreadLocal<Boolean> threadLocalOutputStreamInUse = new ThreadLocal<Boolean>() {
        @Override
        protected Boolean initialValue() {
            return false;
        }
    };

    /**
     * Encodes the given value using the specified Coder, and returns
     * the encoded bytes.
     * <p>
     * <p>This function is not reentrant; it should not be called from methods of the provided
     * {@link Coder}.
     */
    public static <T> byte[] encodeToByteArray(Coder<T> coder, T value)
            throws CoderException {
        if (threadLocalOutputStreamInUse.get()) {
            // encodeToByteArray() is called recursively and the thread local stream is in use,
            // allocating a new one.
            ByteArrayOutputStream stream = new ExposedByteArrayOutputStream();
            encodeToSafeStream(coder, value, stream);
            return stream.toByteArray();
        } else {
            threadLocalOutputStreamInUse.set(true);
            try {
                ByteArrayOutputStream stream = getThreadLocalOutputStream();
                encodeToSafeStream(coder, value, stream);
                return stream.toByteArray();
            } finally {
                threadLocalOutputStreamInUse.set(false);
            }
        }
    }

    /**
     * Encodes {@code value} to the given {@code stream}, which should be a stream that never throws
     * {@code IOException}, such as {@code ByteArrayOutputStream} or
     * {@link ExposedByteArrayOutputStream}.
     */
    private static <T> void encodeToSafeStream(
            Coder<T> coder, T value, OutputStream stream) throws CoderException {
        try {
            coder.encode(value, new UnownedOutputStream(stream));
        } catch (CoderException exn) {
            throw new IllegalArgumentException(
                    "Forbidden IOException when writing to OutputStream", exn);
        }
    }

    /**
     * Decodes the given bytes using the specified Coder, and returns
     * the resulting decoded value.
     */
    public static <T> T decodeFromByteArray(
            Coder<T> coder, byte[] encodedValue) throws CoderException {
        try (ExposedByteArrayInputStream stream = new ExposedByteArrayInputStream(encodedValue)) {
            T result = decodeFromSafeStream(coder, stream);
            if (stream.available() != 0) {
                throw new CoderException(
                        stream.available() + " unexpected extra bytes after decoding " + result);
            }
            return result;
        }
    }

    /**
     * Decodes a value from the given {@code stream}, which should be a stream that never throws
     * {@code IOException}, such as {@code ByteArrayInputStream} or
     * {@link ExposedByteArrayInputStream}.
     */
    private static <T> T decodeFromSafeStream(
            Coder<T> coder, InputStream stream) throws CoderException {
        try {
            return coder.decode(new UnownedInputStream(stream));
        } catch (CoderException exn) {
            throw new IllegalArgumentException(
                    "Forbidden IOException when reading from InputStream", exn);
        }
    }

    private static ByteArrayOutputStream getThreadLocalOutputStream() {
        SoftReference<ExposedByteArrayOutputStream> refStream = threadLocalOutputStream.get();
        ExposedByteArrayOutputStream stream = refStream == null ? null : refStream.get();
        if (stream == null) {
            stream = new ExposedByteArrayOutputStream();
            threadLocalOutputStream.set(new SoftReference<>(stream));
        }
        stream.reset();
        return stream;
    }

    /**
     * Clones the given value by encoding and then decoding it with the specified Coder.
     * <p>
     * <p>This function is not reentrant; it should not be called from methods of the provided
     * {@link Coder}.
     */
    public static <T> T clone(Coder<T> coder, T value) throws CoderException {
        return decodeFromByteArray(coder, encodeToByteArray(coder, value));
    }

    /**
     * Encodes the given value using the specified Coder, and returns the Base64 encoding of the
     * encoded bytes.
     *
     * @throws CoderException if there are errors during encoding.
     */
    public static <T> String encodeToBase64(Coder<T> coder, T value)
            throws CoderException {
        byte[] rawValue = encodeToByteArray(coder, value);
        return BaseEncoding.base64Url().omitPadding().encode(rawValue);
    }

    /**
     * Parses a value from a base64-encoded String using the given coder.
     */
    public static <T> T decodeFromBase64(Coder<T> coder, String encodedValue) throws CoderException {
        return decodeFromSafeStream(
                coder,
                new ByteArrayInputStream(BaseEncoding.base64Url().omitPadding().decode(encodedValue)));
    }

    /**
     * {@link ByteArrayOutputStream} special cased to treat writes of a single byte-array specially.
     * When calling {@link #toByteArray()} after writing only one {@code byte[]} using
     * {@link #writeAndOwn(byte[])}, it will return that array directly.
     */
    public static class ExposedByteArrayOutputStream extends ByteArrayOutputStream {

        private byte[] swappedBuffer;

        /**
         * If true, this stream doesn't allow direct access to the passed in byte-array. It behaves just
         * like a normal {@link ByteArrayOutputStream}.
         * <p>
         * <p>It is set to true after any write operations other than the first call to
         * {@link #writeAndOwn(byte[])}.
         */
        private boolean isFallback = false;

        /**
         * Fall back to the behavior of a normal {@link ByteArrayOutputStream}.
         */
        private void fallback() {
            isFallback = true;
            if (swappedBuffer != null) {
                // swappedBuffer != null means buf is actually provided by the caller of writeAndOwn(),
                // while swappedBuffer is the original buffer.
                // Recover the buffer and copy the bytes from buf.
                byte[] tempBuffer = buf;
                count = 0;
                buf = swappedBuffer;
                super.write(tempBuffer, 0, tempBuffer.length);
                swappedBuffer = null;
            }
        }

        /**
         * Write {@code b} to the stream and take the ownership of {@code b}.
         * If the stream is empty, {@code b} itself will be used as the content of the stream and
         * no content copy will be involved.
         * <p>
         * <p><i>Note: After passing any byte array to this method, it must not be modified again.</i>
         */
        public void writeAndOwn(byte[] b) throws IOException {
            if (b.length == 0) {
                return;
            }
            if (count == 0) {
                // Optimized first-time whole write.
                // The original buffer will be swapped to swappedBuffer, while the input b is used as buf.
                swappedBuffer = buf;
                buf = b;
                count = b.length;
            } else {
                fallback();
                super.write(b);
            }
        }

        @Override
        public void write(byte[] b, int off, int len) {
            fallback();
            super.write(b, off, len);
        }

        @Override
        public void write(int b) {
            fallback();
            super.write(b);
        }

        @Override
        public byte[] toByteArray() {
            // Note: count == buf.length is not a correct criteria to "return buf;", because the internal
            // buf may be reused after reset().
            if (!isFallback && count > 0) {
                return buf;
            } else {
                return super.toByteArray();
            }
        }

        @Override
        public void reset() {
            if (count == 0) {
                return;
            }
            count = 0;
            if (isFallback) {
                isFallback = false;
            } else {
                buf = swappedBuffer;
                swappedBuffer = null;
            }
        }
    }

    /**
     * {@link ByteArrayInputStream} that allows accessing the entire internal buffer without copying.
     */
    public static class ExposedByteArrayInputStream extends ByteArrayInputStream {

        public ExposedByteArrayInputStream(byte[] buf) {
            super(buf);
        }

        /**
         * Read all remaining bytes.
         */
        public byte[] readAll() throws IOException {
            if (pos == 0 && count == buf.length) {
                pos = count;
                return buf;
            }
            byte[] ret = new byte[count - pos];
            super.read(ret);
            return ret;
        }

        @Override
        public void close() {
            try {
                super.close();
            } catch (IOException exn) {
                throw new RuntimeException("Unexpected IOException closing ByteArrayInputStream", exn);
            }
        }
    }

    /**
     * A {@link OutputStream} wrapper which protects against the user attempting to modify
     * the underlying stream by closing it.
     */
    public static class UnownedOutputStream extends FilterOutputStream {
        public UnownedOutputStream(OutputStream delegate) {
            super(delegate);
        }

        @Override
        public void close() throws IOException {
            throw new UnsupportedOperationException("Caller does not own the underlying output stream "
                    + " and should not call close().");
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof UnownedOutputStream
                    && ((UnownedOutputStream) obj).out.equals(out);
        }

        @Override
        public int hashCode() {
            return out.hashCode();
        }

    }

    /**
     * A {@link OutputStream} wrapper which protects against the user attempting to modify
     * the underlying stream by closing it or using mark.
     */
    public static class UnownedInputStream extends FilterInputStream {
        public UnownedInputStream(InputStream delegate) {
            super(delegate);
        }

        @Override
        public void close() throws IOException {
            throw new UnsupportedOperationException("Caller does not own the underlying input stream "
                    + " and should not call close().");
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof UnownedInputStream
                    && ((UnownedInputStream) obj).in.equals(in);
        }

        @Override
        public int hashCode() {
            return in.hashCode();
        }

        @SuppressWarnings("UnsynchronizedOverridesSynchronized")
        @Override
        public void mark(int readlimit) {
            throw new UnsupportedOperationException("Caller does not own the underlying input stream "
                    + " and should not call mark().");
        }

        @Override
        public boolean markSupported() {
            return false;
        }

        @SuppressWarnings("UnsynchronizedOverridesSynchronized")
        @Override
        public void reset() throws IOException {
            throw new UnsupportedOperationException("Caller does not own the underlying input stream "
                    + " and should not call reset().");
        }

    }


}
