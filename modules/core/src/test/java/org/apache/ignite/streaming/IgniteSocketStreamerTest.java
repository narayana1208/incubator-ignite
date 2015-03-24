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
import org.apache.ignite.configuration.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.testframework.junits.common.*;

import java.io.*;
import java.net.*;
import java.util.*;

import static org.apache.ignite.cache.CacheMode.*;

/**
 * Test for data loading using {@link IgniteSocketStreamer}.
 */
public class IgniteSocketStreamerTest extends GridCommonAbstractTest {
    /** Host. */
    private static final String HOST = "localhost";

    /** Port. */
    private static final int PORT = 5555;

    /** Entry count. */
    private static final int ENTRY_CNT = 50000;

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setCacheMode(PARTITIONED);

        ccfg.setBackups(1);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /**
     * Tests data loading.
     */
    public void testLoadData() throws Exception {
        try (Ignite g = startGrid()) {

            IgniteCache<Integer, String> cache = g.jcache(null);

            cache.clear();

            try (IgniteDataStreamer<Integer, String> stmr = g.dataStreamer(null)) {

                startServer();

                IgniteClosure<IgniteBiTuple<Integer, String>, Map.Entry<Integer, String>> converter =
                    new IgniteClosure<IgniteBiTuple<Integer, String>, Map.Entry<Integer, String>>() {
                        @Override public Map.Entry<Integer, String> apply(IgniteBiTuple<Integer, String> input) {
                            return new IgniteBiTuple<>(input.getKey(), input.getValue());
                        }
                    };

                IgniteSocketStreamer<IgniteBiTuple<Integer, String>, Integer, String> sockStmr =
                    new IgniteSocketStreamer<>(HOST, PORT, stmr, converter);

                sockStmr.loadData();
            }

            assertEquals(ENTRY_CNT, cache.size());
        } finally {
            stopAllGrids();
        }
    }

    /**
     * Starts streaming server and writes data into socket.
     */
    private static void startServer() {
        new Thread() {
            @Override public void run() {
                try (ServerSocket srvSock = new ServerSocket(PORT);
                     Socket sock = srvSock.accept();
                     ObjectOutputStream oos =
                         new ObjectOutputStream(new BufferedOutputStream(sock.getOutputStream()))) {

                    for (int i = 0; i < ENTRY_CNT; i++)
                        oos.writeObject(new IgniteBiTuple<>(i, Integer.toString(i)));
                }
                catch (IOException e) {
                    // No-op.
                }
            }
        }.start();
    }
}