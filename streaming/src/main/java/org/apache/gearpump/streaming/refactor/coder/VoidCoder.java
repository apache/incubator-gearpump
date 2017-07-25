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

import java.io.InputStream;
import java.io.OutputStream;

public class VoidCoder extends AtomicCoder<Void> {

    public static VoidCoder of() {
        return INSTANCE;
    }

    /////////////////////////////////////////////////////////////////////////////

    private static final VoidCoder INSTANCE = new VoidCoder();

    private VoidCoder() {
    }

    @Override
    public void encode(Void value, OutputStream outStream) {
        // Nothing to write!
    }

    @Override
    public Void decode(InputStream inStream) {
        // Nothing to read!
        return null;
    }

    @Override
    public void verifyDeterministic() {
    }

    @Override
    public boolean consistentWithEquals() {
        return true;
    }

    @Override
    public boolean isRegisterByteSizeObserverCheap(Void value) {
        return true;
    }

    @Override
    protected long getEncodedElementByteSize(Void value) {
        return 0;
    }
}
