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

import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.io.ByteStreams;
import com.google.common.io.CountingOutputStream;

import java.io.*;
import java.util.*;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * A Coder inspired by Apache Beam
 *
 * A {@link Coder Coder&lt;T&gt;} defines how to encode and decode values of type {@code T} into
 * byte streams.
 *
 * <p>{@link Coder} instances are serialized during job creation and deserialized
 * before use. This will generally be performed by serializing the object via Java Serialization.
 *
 * <p>All methods of a {@link Coder} are required to be thread safe.
 *
 * @param <T> the type of values being encoded and decoded
 */
public abstract class Coder<T> implements Serializable {

    /**
     * Encodes the given value of type {@code T} onto the given output stream.
     *
     * @throws IOException if writing to the {@code OutputStream} fails
     * for some reason
     * @throws CoderException if the value could not be encoded for some reason
     */
    public abstract void encode(T value, OutputStream outStream)
            throws CoderException;

    /**
     * Decodes a value of type {@code T} from the given input stream in
     * the given context.  Returns the decoded value.
     *
     * @throws IOException if reading from the {@code InputStream} fails
     * for some reason
     * @throws CoderException if the value could not be decoded for some reason
     */
    public abstract T decode(InputStream inStream) throws CoderException;

    /**
     * If this is a {@link Coder} for a parameterized type, returns the
     * list of {@link Coder}s being used for each of the parameters in the same order they appear
     * within the parameterized type's type signature. If this cannot be done, or this
     * {@link Coder} does not encode/decode a parameterized type, returns the empty list.
     */
    public abstract List<? extends Coder<?>> getCoderArguments();

    /**
     * Throw {@link NonDeterministicException} if the coding is not deterministic.
     *
     * <p>In order for a {@code Coder} to be considered deterministic,
     * the following must be true:
     * <ul>
     *   <li>two values that compare as equal (via {@code Object.equals()}
     *       or {@code Comparable.compareTo()}, if supported) have the same
     *       encoding.
     *   <li>the {@code Coder} always produces a canonical encoding, which is the
     *       same for an instance of an object even if produced on different
     *       computers at different times.
     * </ul>
     *
     * @throws Coder.NonDeterministicException if this coder is not deterministic.
     */
    public abstract void verifyDeterministic() throws Coder.NonDeterministicException;

    /**
     * Verifies all of the provided coders are deterministic. If any are not, throws a {@link
     * NonDeterministicException} for the {@code target} {@link Coder}.
     */
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

    /**
     * Verifies all of the provided coders are deterministic. If any are not, throws a {@link
     * NonDeterministicException} for the {@code target} {@link Coder}.
     */
    public static void verifyDeterministic(Coder<?> target, String message, Coder<?>... coders)
            throws NonDeterministicException {
        verifyDeterministic(target, message, Arrays.asList(coders));
    }

    /**
     * Returns {@code true} if this {@link Coder} is injective with respect to {@link Objects#equals}.
     *
     * <p>Whenever the encoded bytes of two values are equal, then the original values are equal
     * according to {@code Objects.equals()}. Note that this is well-defined for {@code null}.
     *
     * <p>This condition is most notably false for arrays. More generally, this condition is false
     * whenever {@code equals()} compares object identity, rather than performing a
     * semantic/structural comparison.
     *
     * <p>By default, returns false.
     */
    public boolean consistentWithEquals() {
        return false;
    }

    /**
     * Returns an object with an {@code Object.equals()} method that represents structural equality on
     * the argument.
     *
     * <p>For any two values {@code x} and {@code y} of type {@code T}, if their encoded bytes are the
     * same, then it must be the case that {@code structuralValue(x).equals(@code structuralValue(y)}.
     *
     * <p>Most notably:
     *
     * <ul>
     *   <li>The structural value for an array coder should perform a structural comparison of the
     *       contents of the arrays, rather than the default behavior of comparing according to object
     *       identity.
     *   <li>The structural value for a coder accepting {@code null} should be a proper object with an
     *       {@code equals()} method, even if the input value is {@code null}.
     * </ul>
     *
     * <p>See also {@link #consistentWithEquals()}.
     *
     * <p>By default, if this coder is {@link #consistentWithEquals()}, and the value is not null,
     * returns the provided object. Otherwise, encodes the value into a {@code byte[]}, and returns
     * an object that performs array equality on the encoded bytes.
     */
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

    /**
     * Returns whether {@link #registerByteSizeObserver} cheap enough to
     * call for every element, that is, if this {@code Coder} can
     * calculate the byte size of the element to be coded in roughly
     * constant time (or lazily).
     *
     * <p>Not intended to be called by user code, but instead by
     * {@link PipelineRunner}
     * implementations.
     *
     * <p>By default, returns false. The default {@link #registerByteSizeObserver} implementation
     *         invokes {@link #getEncodedElementByteSize} which requires re-encoding an element
     *         unless it is overridden. This is considered expensive.
     */
    public boolean isRegisterByteSizeObserverCheap(T value) {
        return false;
    }

    /**
     * Notifies the {@code ElementByteSizeObserver} about the byte size
     * of the encoded value using this {@code Coder}.
     *
     * <p>Not intended to be called by user code, but instead by
     * {@link PipelineRunner}
     * implementations.
     *
     * <p>By default, this notifies {@code observer} about the byte size
     * of the encoded value using this coder as returned by {@link #getEncodedElementByteSize}.
     */
    public void registerByteSizeObserver(T value, ElementByteSizeObserver observer) {
        observer.update(getEncodedElementByteSize(value));
    }

    /**
     * Returns the size in bytes of the encoded value using this coder.
     */
    protected long getEncodedElementByteSize(T value) {
        try (CountingOutputStream os = new CountingOutputStream(ByteStreams.nullOutputStream())) {
            encode(value, os);
            return os.getCount();
        } catch (Exception exn) {
            throw new IllegalArgumentException(
                    "Unable to encode element '" + value + "' with coder '" + this + "'.", exn);
        }
    }

    /**
     * Exception thrown by {@link Coder#verifyDeterministic()} if the encoding is
     * not deterministic, including details of why the encoding is not deterministic.
     */
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

    /**
     * An observer that gets notified when an observable iterator
     * returns a new value. This observer just notifies an outerObserver
     * about this event. Additionally, the outerObserver is notified
     * about additional separators that are transparently added by this
     * coder.
     */
    private class IteratorObserver implements Observer {
        private final IterableLikeCoder.ElementByteSizeObserver outerObserver;
        private final boolean countable;

        public IteratorObserver(IterableLikeCoder.ElementByteSizeObserver outerObserver,
                                boolean countable) {
            this.outerObserver = outerObserver;
            this.countable = countable;

            if (countable) {
                // Additional 4 bytes are due to size.
                outerObserver.update(4L);
            } else {
                // Additional 5 bytes are due to size = -1 (4 bytes) and
                // hasNext = false (1 byte).
                outerObserver.update(5L);
            }
        }

        @Override
        public void update(Observable obs, Object obj) {
            if (!(obj instanceof Long)) {
                throw new AssertionError("unexpected parameter object");
            }

            if (countable) {
                outerObserver.update(obs, obj);
            } else {
                // Additional 1 byte is due to hasNext = true flag.
                outerObserver.update(obs, 1 + (long) obj);
            }
        }
    }

    /**
     * An observer that gets notified when additional bytes are read and/or used.
     */
    public static abstract class ElementByteSizeObserver implements Observer {
        private boolean isLazy = false;
        private long totalSize = 0;
        private double scalingFactor = 1.0;

        public ElementByteSizeObserver() {}

        /**
         * Called to report element byte size.
         */
        protected abstract void reportElementSize(long elementByteSize);

        /**
         * Sets byte counting for the current element as lazy. That is, the
         * observer will get notified of the element's byte count only as
         * element's pieces are being processed or iterated over.
         */
        public void setLazy() {
            isLazy = true;
        }

        /**
         * Returns whether byte counting for the current element is lazy, that is,
         * whether the observer gets notified of the element's byte count only as
         * element's pieces are being processed or iterated over.
         */
        public boolean getIsLazy() {
            return isLazy;
        }

        /**
         * Updates the observer with a context specified, but without an instance of
         * the Observable.
         */
        public void update(Object obj) {
            update(null, obj);
        }

        /**
         * Sets a multiplier to use on observed sizes.
         */
        public void setScalingFactor(double scalingFactor) {
            this.scalingFactor = scalingFactor;
        }

        @Override
        public void update(Observable obs, Object obj) {
            if (obj instanceof Long) {
                totalSize += scalingFactor * (Long) obj;
            } else if (obj instanceof Integer) {
                totalSize += scalingFactor * (Integer) obj;
            } else {
                throw new AssertionError("unexpected parameter object");
            }
        }

        /**
         * Advances the observer to the next element. Adds the current total byte
         * size to the counter, and prepares the observer for the next element.
         */
        public void advance() {
            reportElementSize(totalSize);
            totalSize = 0;
            isLazy = false;
        }
    }

    /**
     * An abstract class used for iterables that notify observers about size in
     * bytes of their elements, as they are being iterated over.
     *
     * @param <V> the type of elements returned by this iterable
     * @param <InputT> type type of iterator returned by this iterable
     */
    public static abstract class ElementByteSizeObservableIterable<
            V, InputT extends IterableLikeCoder.ElementByteSizeObservableIterator<V>>
            implements Iterable<V> {
        private List<Observer> observers = new ArrayList<>();

        /**
         * Derived classes override this method to return an iterator for this
         * iterable.
         */
        protected abstract InputT createIterator();

        /**
         * Sets the observer, which will observe the iterator returned in
         * the next call to iterator() method. Future calls to iterator()
         * won't be observed, unless an observer is set again.
         */
        public void addObserver(Observer observer) {
            observers.add(observer);
        }

        /**
         * Returns a new iterator for this iterable. If an observer was set in
         * a previous call to setObserver(), it will observe the iterator returned.
         */
        @Override
        public InputT iterator() {
            InputT iterator = createIterator();
            for (Observer observer : observers) {
                iterator.addObserver(observer);
            }
            observers.clear();
            return iterator;
        }
    }

    /**
     * An abstract class used for iterators that notify observers about size in
     * bytes of their elements, as they are being iterated over. The subclasses
     * need to implement the standard Iterator interface and call method
     * notifyValueReturned() for each element read and/or iterated over.
     *
     * @param <V> value type
     */
    public static abstract class ElementByteSizeObservableIterator<V>
            extends Observable implements Iterator<V> {
        protected final void notifyValueReturned(long byteSize) {
            setChanged();
            notifyObservers(byteSize);
        }
    }
}
