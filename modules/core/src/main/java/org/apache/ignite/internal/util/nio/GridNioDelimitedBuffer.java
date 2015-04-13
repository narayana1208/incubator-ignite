/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.util.nio;

import org.jetbrains.annotations.*;

import java.nio.*;
import java.util.*;

/**
 * Buffer with message delimiter support.
 */
public class GridNioDelimitedBuffer {
    /** Buffer size. */
    private static final int BUFFER_SIZE = 512;

    /** Delimiter. */
    private final byte[] delim;

    /** Data. */
    private byte[] data;

    /** Count. */
    private int cnt;

    /**
     * @param delim Delimiter.
     */
    public GridNioDelimitedBuffer(byte[] delim) {
        assert delim != null;
        assert delim.length > 0;

        this.delim = delim;

        reset();
    }

    /**
     * Resets buffer state.
     */
    private void reset() {
        cnt = 0;

        data = new byte[BUFFER_SIZE];
    }

    /**
     * @param buf Buffer.
     * @return Message bytes or {@code null} if message is not fully read yet.
     */
    @Nullable public byte[] read(ByteBuffer buf) {
        for(; buf.hasRemaining();) {

            if (cnt == data.length)
                data = Arrays.copyOf(data, data.length * 2);

            data[cnt++] = buf.get();

            if (cnt >= delim.length && found()) {
                byte[] bytes = Arrays.copyOfRange(data, 0, cnt - delim.length);

                reset();

                return bytes;
            }
        }

        return null;
    }

    /**
     * Tries find delimiter in buffer.
     *
     * @return {@code True} if delimiter found, {@code false} - otherwise.
     */
    private boolean found() {
        int from = cnt - delim.length;

        for (int i = 0; i < delim.length ; i++) {
            if (delim[i] != data[from + i])
                return false;
        }

        return true;
    }
}
