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

public class DoubleCoder extends AtomicCoder<Double> {

    public static DoubleCoder of() {
        return INSTANCE;
    }

    /////////////////////////////////////////////////////////////////////////////

    private static final DoubleCoder INSTANCE = new DoubleCoder();

    private DoubleCoder() {
    }

    @Override
    public void encode(Double value, OutputStream outStream)
            throws CoderException {
        if (value == null) {
            throw new CoderException("cannot encode a null Double");
        }
        try {
            new DataOutputStream(outStream).writeDouble(value);
        } catch (IOException e) {
            throw new CoderException(e);
        }
    }

    @Override
    public Double decode(InputStream inStream)
            throws CoderException {
        try {
            return new DataInputStream(inStream).readDouble();
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
        throw new NonDeterministicException(this,
                "Floating point encodings are not guaranteed to be deterministic.");
    }

    @Override
    public boolean consistentWithEquals() {
        return true;
    }

    @Override
    public boolean isRegisterByteSizeObserverCheap(Double value) {
        return true;
    }

    @Override
    protected long getEncodedElementByteSize(Double value) {
        if (value == null) {
            throw new CoderException("cannot encode a null Double");
        }
        return 8;
    }
}
