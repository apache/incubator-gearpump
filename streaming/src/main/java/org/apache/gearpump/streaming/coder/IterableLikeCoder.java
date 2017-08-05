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

import java.io.*;
import java.util.*;

import static com.google.common.base.Preconditions.checkArgument;

public abstract class IterableLikeCoder<T, IterableT extends Iterable<T>>
        extends StructuredCoder<IterableT> {
    public Coder<T> getElemCoder() {
        return elementCoder;
    }

    protected abstract IterableT decodeToIterable(List<T> decodedElements);

    /////////////////////////////////////////////////////////////////////////////
    // Internal operations below here.

    private final Coder<T> elementCoder;
    private final String iterableName;

    protected IterableLikeCoder(Coder<T> elementCoder, String  iterableName) {
        checkArgument(elementCoder != null, "element Coder for IterableLikeCoder must not be null");
        checkArgument(iterableName != null, "iterable name for IterableLikeCoder must not be null");
        this.elementCoder = elementCoder;
        this.iterableName = iterableName;
    }

    @Override
    public void encode(IterableT iterable, OutputStream outStream) {
        if (iterable == null) {
            throw new CoderException("cannot encode a null " + iterableName);
        }
        DataOutputStream dataOutStream = new DataOutputStream(outStream);
        try {
            if (iterable instanceof Collection) {
                // We can know the size of the Iterable.  Use an encoding with a
                // leading size field, followed by that many elements.
                Collection<T> collection = (Collection<T>) iterable;
                dataOutStream.writeInt(collection.size());
                for (T elem : collection) {
                    elementCoder.encode(elem, dataOutStream);
                }
            } else {
                // We don't know the size without traversing it so use a fixed size buffer
                // and encode as many elements as possible into it before outputting the size followed
                // by the elements.
                dataOutStream.writeInt(-1);
                BufferedElementCountingOutputStream countingOutputStream =
                        new BufferedElementCountingOutputStream(dataOutStream);
                for (T elem : iterable) {
                    countingOutputStream.markElementStart();
                    elementCoder.encode(elem, countingOutputStream);
                }
                countingOutputStream.finish();
            }
            // Make sure all our output gets pushed to the underlying outStream.
            dataOutStream.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public IterableT decode(InputStream inStream) {
        try {
            DataInputStream dataInStream = new DataInputStream(inStream);
            int size = dataInStream.readInt();
            if (size >= 0) {
                List<T> elements = new ArrayList<>(size);
                for (int i = 0; i < size; i++) {
                    elements.add(elementCoder.decode(dataInStream));
                }
                return decodeToIterable(elements);
            }
            List<T> elements = new ArrayList<>();
            // We don't know the size a priori.  Check if we're done with
            // each block of elements.
            long count = VarInt.decodeLong(dataInStream);
            while (count > 0L) {
                elements.add(elementCoder.decode(dataInStream));
                --count;
                if (count == 0L) {
                    count = VarInt.decodeLong(dataInStream);
                }
            }
            return decodeToIterable(elements);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
        return Arrays.asList(elementCoder);
    }

    @Override
    public void verifyDeterministic() throws Coder.NonDeterministicException {
        throw new NonDeterministicException(this,
                "IterableLikeCoder can not guarantee deterministic ordering.");
    }

    @Override
    public boolean isRegisterByteSizeObserverCheap(
            IterableT iterable) {
        return iterable instanceof ElementByteSizeObservableIterable;
    }

    @Override
    public void registerByteSizeObserver(
            IterableT iterable, ElementByteSizeObserver observer) {
        if (iterable == null) {
            throw new CoderException("cannot encode a null Iterable");
        }

        if (iterable instanceof ElementByteSizeObservableIterable) {
            observer.setLazy();
            ElementByteSizeObservableIterable<?, ?> observableIterable =
                    (ElementByteSizeObservableIterable<?, ?>) iterable;
            observableIterable.addObserver(
                    new IteratorObserver(observer, iterable instanceof Collection));
        } else {
            if (iterable instanceof Collection) {
                // We can know the size of the Iterable.  Use an encoding with a
                // leading size field, followed by that many elements.
                Collection<T> collection = (Collection<T>) iterable;
                observer.update(4L);
                for (T elem : collection) {
                    elementCoder.registerByteSizeObserver(elem, observer);
                }
            } else {
                // TODO: (BEAM-1537) Update to use an accurate count depending on size and count,
                // currently we are under estimating the size by up to 10 bytes per block of data since we
                // are not encoding the count prefix which occurs at most once per 64k of data and is upto
                // 10 bytes long. Since we include the total count we can upper bound the underestimate
                // to be 10 / 65536 ~= 0.0153% of the actual size.
                observer.update(4L);
                long count = 0;
                for (T elem : iterable) {
                    count += 1;
                    elementCoder.registerByteSizeObserver(elem, observer);
                }
                if (count > 0) {
                    // Update the length based upon the number of counted elements, this helps
                    // eliminate the case where all the elements are encoded in the first block and
                    // it is quite short (e.g. Long.MAX_VALUE nulls encoded with VoidCoder).
                    observer.update(VarInt.getLength(count));
                }
                // Update with the terminator byte.
                observer.update(1L);
            }
        }
    }

    private class IteratorObserver implements Observer {
        private final ElementByteSizeObserver outerObserver;
        private final boolean countable;

        public IteratorObserver(ElementByteSizeObserver outerObserver,
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

    public static abstract class ElementByteSizeObservableIterable<
            V, InputT extends ElementByteSizeObservableIterator<V>>
            implements Iterable<V> {
        private List<Observer> observers = new ArrayList<>();

        protected abstract InputT createIterator();

        public void addObserver(Observer observer) {
            observers.add(observer);
        }

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

    public static abstract class ElementByteSizeObservableIterator<V>
            extends Observable implements Iterator<V> {
        protected final void notifyValueReturned(long byteSize) {
            setChanged();
            notifyObservers(byteSize);
        }
    }

}
