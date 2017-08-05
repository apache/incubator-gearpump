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

import com.google.common.base.Joiner;
import com.google.common.io.ByteStreams;
import com.google.common.io.CountingOutputStream;

import java.io.*;
import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

public abstract class Coder<T> implements Serializable {

    public abstract void encode(T value, OutputStream outStream)
            throws CoderException;

    public abstract T decode(InputStream inStream) throws CoderException;

    public abstract List<? extends Coder<?>> getCoderArguments();

    public abstract void verifyDeterministic() throws Coder.NonDeterministicException;

    public static void verifyDeterministic(Coder<?> target, String message, Iterable<Coder<?>> coders)
            throws NonDeterministicException {
        for (Coder<?> coder : coders) {
            try {
                coder.verifyDeterministic();
            } catch (NonDeterministicException e) {
                throw new NonDeterministicException(target, message, e);
            }
        }
    }

    public static void verifyDeterministic(Coder<?> target, String message, Coder<?>... coders)
            throws NonDeterministicException {
        verifyDeterministic(target, message, Arrays.asList(coders));
    }

    public boolean consistentWithEquals() {
        return false;
    }

    public Object structuralValue(T value) {
        if (value != null && consistentWithEquals()) {
            return value;
        } else {
            try {
                ByteArrayOutputStream os = new ByteArrayOutputStream();
                encode(value, os);
                return new StructuralByteArray(os.toByteArray());
            } catch (Exception exn) {
                throw new IllegalArgumentException(
                        "Unable to encode element '" + value + "' with coder '" + this + "'.", exn);
            }
        }
    }

    public boolean isRegisterByteSizeObserverCheap(T value) {
        return false;
    }

    public void registerByteSizeObserver(T value, ElementByteSizeObserver observer) {
        observer.update(getEncodedElementByteSize(value));
    }

    protected long getEncodedElementByteSize(T value) {
        try (CountingOutputStream os = new CountingOutputStream(ByteStreams.nullOutputStream())) {
            encode(value, os);
            return os.getCount();
        } catch (Exception exn) {
            throw new IllegalArgumentException(
                    "Unable to encode element '" + value + "' with coder '" + this + "'.", exn);
        }
    }

    public static class NonDeterministicException extends RuntimeException {
        private Coder<?> coder;
        private List<String> reasons;

        public NonDeterministicException(
                Coder<?> coder, String reason, NonDeterministicException e) {
            this(coder, Arrays.asList(reason), e);
        }

        public NonDeterministicException(Coder<?> coder, String reason) {
            this(coder, Arrays.asList(reason), null);
        }

        public NonDeterministicException(Coder<?> coder, List<String> reasons) {
            this(coder, reasons, null);
        }

        public NonDeterministicException(
                Coder<?> coder,
                List<String> reasons,
                NonDeterministicException cause) {
            super(cause);
            checkArgument(reasons.size() > 0, "Reasons must not be empty.");
            this.reasons = reasons;
            this.coder = coder;
        }

        public Iterable<String> getReasons() {
            return reasons;
        }

        @Override
        public String getMessage() {
            return String.format("%s is not deterministic because:%n  %s",
                    coder, Joiner.on("%n  ").join(reasons));
        }
    }
}
