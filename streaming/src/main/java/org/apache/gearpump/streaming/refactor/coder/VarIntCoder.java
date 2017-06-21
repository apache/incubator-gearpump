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
 * A {@link Coder} that encodes {@link Integer Integers} using between 1 and 5 bytes. Negative
 * numbers always take 5 bytes, so {@link BigEndianIntegerCoder} may be preferable for
 * integers that are known to often be large or negative.
 */
public class VarIntCoder extends AtomicCoder<Integer> {

    public static VarIntCoder of() {
        return INSTANCE;
    }

    /////////////////////////////////////////////////////////////////////////////

    private static final VarIntCoder INSTANCE = new VarIntCoder();

    private VarIntCoder() {}

    @Override
    public void encode(Integer value, OutputStream outStream)
            throws CoderException {
        if (value == null) {
            throw new CoderException("cannot encode a null Integer");
        }
        try {
            VarInt.encode(value.intValue(), outStream);
        } catch (IOException e) {
            throw new CoderException(e);
        }
    }

    @Override
    public Integer decode(InputStream inStream)
            throws CoderException {
        try {
            return VarInt.decodeInt(inStream);
        } catch (EOFException | UTFDataFormatException exn) {
            // These exceptions correspond to decoding problems, so change
            // what kind of exception they're branded as.
            throw new CoderException(exn);
        } catch (Exception e) {
            throw new CoderException(e);
        }
    }

    @Override
    public void verifyDeterministic() {}

    /**
     * {@inheritDoc}
     *
     * @return {@code true}. {@link VarIntCoder} is injective.
     */
    @Override
    public boolean consistentWithEquals() {
        return true;
    }

    /**
     * {@inheritDoc}
     *
     * @return {@code true}. {@link #getEncodedElementByteSize} is cheap.
     */
    @Override
    public boolean isRegisterByteSizeObserverCheap(Integer value) {
        return true;
    }

    @Override
    public long getEncodedElementByteSize(Integer value) {
        if (value == null) {
            throw new CoderException("cannot encode a null Integer");
        }
        return VarInt.getLength(value.longValue());
    }
}
