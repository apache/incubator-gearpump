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

import java.io.*;

/**
 * A {@link BigEndianLongCoder} encodes {@link Long}s in 8 bytes, big-endian.
 * inspired by Apache Beam
 */
public class BigEndianLongCoder extends AtomicCoder<Long> {

    public static BigEndianLongCoder of() {
        return INSTANCE;
    }

    /////////////////////////////////////////////////////////////////////////////

    private static final BigEndianLongCoder INSTANCE = new BigEndianLongCoder();

    private BigEndianLongCoder() {}

    @Override
    public void encode(Long value, OutputStream outStream)
            throws CoderException {
        if (value == null) {
            throw new CoderException("cannot encode a null Long");
        }
        try {
            new DataOutputStream(outStream).writeLong(value);
        } catch (IOException e) {
            throw new CoderException(e);
        }
    }

    @Override
    public Long decode(InputStream inStream)
            throws CoderException {
        try {
            return new DataInputStream(inStream).readLong();
        } catch (EOFException | UTFDataFormatException exn) {
            // These exceptions correspond to decoding problems, so change
            // what kind of exception they're branded as.
            throw new CoderException(exn);
        } catch (IOException e) {
            throw new CoderException(e);
        }
    }

    @Override
    public void verifyDeterministic() {
    }

    /**
     * {@inheritDoc}
     *
     * @return {@code true}. This coder is injective.
     */
    @Override
    public boolean consistentWithEquals() {
        return true;
    }

    /**
     * {@inheritDoc}
     *
     * @return {@code true}, since {@link #getEncodedElementByteSize} returns a constant.
     */
    @Override
    public boolean isRegisterByteSizeObserverCheap(Long value) {
        return true;
    }

    /**
     * {@inheritDoc}
     *
     * @return {@code 8}, the byte size of a big-endian encoded {@code Long}.
     */
    @Override
    protected long getEncodedElementByteSize(Long value) {
        if (value == null) {
            throw new CoderException("cannot encode a null Long");
        }
        return 8;
    }
}
