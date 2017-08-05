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

package org.apache.gearpump.streaming.coder;

import com.google.common.io.BaseEncoding;

import java.io.*;
import java.lang.ref.SoftReference;

public final class CoderUtils {
    private CoderUtils() {
    }  // Non-instantiable

    private static ThreadLocal<SoftReference<ExposedByteArrayOutputStream>>
            threadLocalOutputStream = new ThreadLocal<>();

    private static ThreadLocal<Boolean> threadLocalOutputStreamInUse = new ThreadLocal<Boolean>() {
        @Override
        protected Boolean initialValue() {
            return false;
        }
    };

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

    private static <T> void encodeToSafeStream(
            Coder<T> coder, T value, OutputStream stream) throws CoderException {
        try {
            coder.encode(value, new UnownedOutputStream(stream));
        } catch (CoderException exn) {
            throw new IllegalArgumentException(
                    "Forbidden IOException when writing to OutputStream", exn);
        }
    }

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

    public static <T> T clone(Coder<T> coder, T value) throws CoderException {
        return decodeFromByteArray(coder, encodeToByteArray(coder, value));
    }

    public static <T> String encodeToBase64(Coder<T> coder, T value)
            throws CoderException {
        byte[] rawValue = encodeToByteArray(coder, value);
        return BaseEncoding.base64Url().omitPadding().encode(rawValue);
    }

    public static <T> T decodeFromBase64(Coder<T> coder, String encodedValue) throws CoderException {
        return decodeFromSafeStream(
                coder,
                new ByteArrayInputStream(BaseEncoding.base64Url().omitPadding().decode(encodedValue)));
    }

    public static class ExposedByteArrayOutputStream extends ByteArrayOutputStream {

        private byte[] swappedBuffer;

        private boolean isFallback = false;

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

    public static class ExposedByteArrayInputStream extends ByteArrayInputStream {

        public ExposedByteArrayInputStream(byte[] buf) {
            super(buf);
        }

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
