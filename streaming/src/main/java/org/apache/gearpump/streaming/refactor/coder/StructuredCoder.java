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

import java.io.ByteArrayOutputStream;
import java.util.Collections;
import java.util.List;

public abstract class StructuredCoder<T> extends Coder<T> {
    protected StructuredCoder() {}

    public List<? extends Coder<?>> getComponents() {
        List<? extends Coder<?>> coderArguments = getCoderArguments();
        if (coderArguments == null) {
            return Collections.emptyList();
        } else {
            return coderArguments;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || this.getClass() != o.getClass()) {
            return false;
        }
        StructuredCoder<?> that = (StructuredCoder<?>) o;
        return this.getComponents().equals(that.getComponents());
    }

    @Override
    public int hashCode() {
        return getClass().hashCode() * 31 + getComponents().hashCode();
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        String s = getClass().getName();
        builder.append(s.substring(s.lastIndexOf('.') + 1));

        List<? extends Coder<?>> componentCoders = getComponents();
        if (!componentCoders.isEmpty()) {
            builder.append('(');
            boolean first = true;
            for (Coder<?> componentCoder : componentCoders) {
                if (first) {
                    first = false;
                } else {
                    builder.append(',');
                }
                builder.append(componentCoder.toString());
            }
            builder.append(')');
        }
        return builder.toString();
    }

    @Override
    public boolean consistentWithEquals() {
        return false;
    }

    @Override
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

}
