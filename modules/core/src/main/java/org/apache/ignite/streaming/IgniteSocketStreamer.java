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

package org.apache.ignite.streaming;

import org.apache.ignite.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;

import java.io.*;
import java.net.*;
import java.util.*;

public class IgniteSocketStreamer<K, V> {
    /** Target streamer. */
    private final IgniteDataStreamer<K, V> streamer;

    /** Stream to entries iterator transformer. */
    private final IgniteClosure<InputStream, Iterator<Map.Entry<K, V>>> f;

    /** Host. */
    private final String host;

    /** Port. */
    private final int port;

    /**
     * Constructs socket streamer.
     *
     * @param host Host.
     * @param port Port.
     * @param streamer Streamer.
     * @param f Stream to entries iterator transformer.
     */
    public IgniteSocketStreamer(
            String host,
            int port,
            IgniteDataStreamer<K, V> streamer,
            IgniteClosure<InputStream, Iterator<Map.Entry<K, V>>> f
            ) {

        A.notNull(streamer, "streamer is null");
        A.notNull(host, "host is null");
        A.notNull(f, "f is null");

        this.host = host;
        this.port = port;
        this.streamer = streamer;
        this.f = f;
    }

    /**
     * Performs loading of data stream.
     */
    public void loadData() {
        try (Socket sock = new Socket(host, port);
             InputStream is = sock.getInputStream()) {

            for (Iterator<Map.Entry<K, V>> it = f.apply(is); it.hasNext();)
                streamer.addData(it.next());
        }
        catch (IOException e) {
            throw new IgniteException(e);
        }
    }

    /**
     * Base iterator implementation with next element pre-fetching.
     *
     * @param <E> Element type.
     */
    abstract static class NextIterator<E> implements Iterator<E>, AutoCloseable {
        /** Closed. */
        private boolean closed;
        /** Next value. */
        private E next;
        /** Next value is available. */
        private boolean gotNext;

        /** {@inheritDoc} */
        @Override public boolean hasNext() {
            if (!closed && !gotNext) {
                next = getNext();

                gotNext = true;
            }

            return !closed;
        }

        /** {@inheritDoc} */
        @Override public E next() {
            if (!hasNext())
                throw new NoSuchElementException("End of stream");

            gotNext = false;

            return next;
        }

        /** {@inheritDoc} */
        @Override public void remove() {
            throw new UnsupportedOperationException();
        }

        /**
         * Gets next value from backed iterator. If next element isn't available the derived class should
         * invoke {@link #close()} method and return any value.
         *
         * @return Next element or closes the iterator.
         */
        protected abstract E getNext();

        /** {@inheritDoc} */
        @Override public void close() {
            closed = true;
        }
    }
}
