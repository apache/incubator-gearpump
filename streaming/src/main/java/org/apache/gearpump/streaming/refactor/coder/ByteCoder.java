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

public class ByteCoder extends AtomicCoder<Byte> {

    public static ByteCoder of() {
        return INSTANCE;
    }

    /////////////////////////////////////////////////////////////////////////////

    private static final ByteCoder INSTANCE = new ByteCoder();

    private ByteCoder() {
    }

    @Override
    public void encode(Byte value, OutputStream outStream)
            throws CoderException {
        if (value == null) {
            throw new CoderException("cannot encode a null Byte");
        }
        try {
            outStream.write(value.byteValue());
        } catch (IOException e) {
            throw new CoderException(e);
        }
    }

    @Override
    public Byte decode(InputStream inStream) throws CoderException {
        try {
            // value will be between 0-255, -1 for EOF
            int value = inStream.read();
            if (value == -1) {
                throw new EOFException("EOF encountered decoding 1 byte from input stream");
            }
            return (byte) value;
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

    @Override
    public boolean consistentWithEquals() {
        return true;
    }

    @Override
    public boolean isRegisterByteSizeObserverCheap(Byte value) {
        return true;
    }

    @Override
    protected long getEncodedElementByteSize(Byte value) {
        if (value == null) {
            throw new CoderException("cannot estimate size for unsupported null value");
        }
        return 1;
    }
}
