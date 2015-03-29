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

/**
 * Data streamer is responsible for streaming data from socket into cache. Every object obtained from socket treats as
 * {@link String} instance and converts to key-value pair using converter.
 *
 * @param <K> Cache entry key type.
 * @param <V> Cache entry value type.
 */
public class IgniteTextSocketStreamer<K, V> extends Receiver<String, K, V> {
    /** Host. */
    private final String host;

    /** Port. */
    private final int port;

    /**
     * Constructs text socket streamer.
     *
     * @param host Host.
     * @param port Port.
     * @param streamer Streamer.
     * @param converter Stream to entries converter.
     */
    public IgniteTextSocketStreamer(
        String host,
        int port,
        IgniteDataStreamer<K, V> streamer,
        IgniteClosure<String, Map.Entry<K, V>> converter
    ) {
        super(streamer, converter);

        A.notNull(host, "host is null");

        this.host = host;
        this.port = port;
    }

    /** {@inheritDoc} */
    @Override protected void receive() {
        try (Socket sock = new Socket(host, port)) {
            loadData(sock);
        }
        catch (Exception e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    private void loadData(Socket sock) throws IOException {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(sock.getInputStream(), "UTF-8"))) {
            String val;

            while (!isStopped() && (val = reader.readLine()) != null)
                addData(val);
        }
    }
}
