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

import java.util.Observable;
import java.util.Observer;

public class IteratorObserver implements Observer {
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
