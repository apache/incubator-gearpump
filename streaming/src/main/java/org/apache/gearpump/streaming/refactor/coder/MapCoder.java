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

import com.google.common.collect.Maps;

import java.io.*;
import java.util.*;

/**
 * Created by yanghua on 5/31/17.
 */
public class MapCoder<K, V> extends StructuredCoder<Map<K, V>> {

    /**
     * Produces a MapCoder with the given keyCoder and valueCoder.
     */
    public static <K, V> MapCoder<K, V> of(
            Coder<K> keyCoder,
            Coder<V> valueCoder) {
        return new MapCoder<>(keyCoder, valueCoder);
    }

    public Coder<K> getKeyCoder() {
        return keyCoder;
    }

    public Coder<V> getValueCoder() {
        return valueCoder;
    }

    /////////////////////////////////////////////////////////////////////////////

    private Coder<K> keyCoder;
    private Coder<V> valueCoder;

    private MapCoder(Coder<K> keyCoder, Coder<V> valueCoder) {
        this.keyCoder = keyCoder;
        this.valueCoder = valueCoder;
    }

    @Override
    public void encode(Map<K, V> map, OutputStream outStream) throws CoderException  {
        if (map == null) {
            throw new CoderException("cannot encode a null Map");
        }
        DataOutputStream dataOutStream = new DataOutputStream(outStream);

        int size = map.size();
        try {
            dataOutStream.writeInt(size);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        if (size == 0) {
            return;
        }

        // Since we handled size == 0 above, entry is guaranteed to exist before and after loop
        Iterator<Map.Entry<K, V>> iterator = map.entrySet().iterator();
        Map.Entry<K, V> entry = iterator.next();
        while (iterator.hasNext()) {
            keyCoder.encode(entry.getKey(), outStream);
            valueCoder.encode(entry.getValue(), outStream);
            entry = iterator.next();
        }

        keyCoder.encode(entry.getKey(), outStream);
        valueCoder.encode(entry.getValue(), outStream);
        // no flush needed as DataOutputStream does not buffer
    }

    @Override
    public Map<K, V> decode(InputStream inStream) {
        DataInputStream dataInStream = new DataInputStream(inStream);
        int size = 0;
        try {
            size = dataInStream.readInt();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        if (size == 0) {
            return Collections.emptyMap();
        }

        Map<K, V> retval = Maps.newHashMapWithExpectedSize(size);
        for (int i = 0; i < size - 1; ++i) {
            K key = keyCoder.decode(inStream);
            V value = valueCoder.decode(inStream);
            retval.put(key, value);
        }

        K key = keyCoder.decode(inStream);
        V value = valueCoder.decode(inStream);
        retval.put(key, value);
        return retval;
    }

    /**
     * {@inheritDoc}
     *
     * @return a {@link List} containing the key coder at index 0 at the and value coder at index 1.
     */
    @Override
    public List<? extends Coder<?>> getCoderArguments() {
        return Arrays.asList(keyCoder, valueCoder);
    }

    /**
     * {@inheritDoc}
     *
     * @throws NonDeterministicException always. Not all maps have a deterministic encoding.
     * For example, {@code HashMap} comparison does not depend on element order, so
     * two {@code HashMap} instances may be equal but produce different encodings.
     */
    @Override
    public void verifyDeterministic() throws NonDeterministicException {
        throw new NonDeterministicException(this,
                "Ordering of entries in a Map may be non-deterministic.");
    }

    @Override
    public void registerByteSizeObserver(
            Map<K, V> map, IterableLikeCoder.ElementByteSizeObserver observer) {
        observer.update(4L);
        if (map.isEmpty()) {
            return;
        }
        Iterator<Map.Entry<K, V>> entries = map.entrySet().iterator();
        Map.Entry<K, V> entry = entries.next();
        while (entries.hasNext()) {
            keyCoder.registerByteSizeObserver(entry.getKey(), observer);
            valueCoder.registerByteSizeObserver(entry.getValue(), observer);
            entry = entries.next();
        }
        keyCoder.registerByteSizeObserver(entry.getKey(), observer);
        valueCoder.registerByteSizeObserver(entry.getValue(), observer);
    }

}
