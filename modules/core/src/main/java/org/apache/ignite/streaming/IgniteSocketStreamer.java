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
import org.apache.ignite.internal.util.future.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;

import java.io.*;
import java.net.*;
import java.util.*;

/**
 * Data streamer is responsible for streaming data from socket into cache. Every object obtained from socket converts
 * to key-value pair using converter.
 *
 * @param <E> Type of element obtained from socket.
 * @param <K> Cache entry key type.
 * @param <V> Cache entry value type.
 */
public class IgniteSocketStreamer<E, K, V> {
    /** Host. */
    private final String host;

    /** Port. */
    private final int port;

    /** Target streamer. */
    protected final IgniteDataStreamer<K, V> streamer;

    /** Stream to entries iterator transformer. */
    protected final IgniteClosure<E, Map.Entry<K, V>> converter;

    /**
     * Constructs socket streamer.
     *
     * @param host Host.
     * @param port Port.
     * @param streamer Streamer.
     * @param converter Stream to entry converter.
     */
    public IgniteSocketStreamer(
        String host,
        int port,
        IgniteDataStreamer<K, V> streamer,
        IgniteClosure<E, Map.Entry<K, V>> converter
    ) {
        A.notNull(streamer, "streamer is null");
        A.notNull(host, "host is null");
        A.notNull(converter, "converter is null");

        this.host = host;
        this.port = port;
        this.streamer = streamer;
        this.converter = converter;
    }

    /**
     * Performs loading of data stream.
     */
    public void loadData() {
        try (Socket sock = new Socket(host, port)) {
            loadData(sock);
        }
        catch (Exception e) {
            throw new IgniteException(e);
        }
    }

    /**
     * Reads data from socket and loads them into target data stream.
     *
     * @param sock Socket.
     */
    @SuppressWarnings("unchecked")
    protected void loadData(Socket sock) throws IOException {
        try (ObjectInputStream ois = new ObjectInputStream(new BufferedInputStream(sock.getInputStream()))) {
            while (true) {
                try {
                    E element = (E) ois.readObject();

                    streamer.addData(converter.apply(element));
                }
                catch (EOFException e) {
                    break;
                }
                catch (IOException | ClassNotFoundException e) {
                    throw new IgniteException(e);
                }
            }
        }
    }
}
