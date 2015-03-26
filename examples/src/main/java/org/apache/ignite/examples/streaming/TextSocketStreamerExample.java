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

package org.apache.ignite.examples.streaming;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.examples.*;
import org.apache.ignite.examples.datagrid.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.streaming.*;

import java.io.*;
import java.net.*;
import java.util.*;

/**
 * Demonstrates how cache can be populated with data utilizing {@link IgniteTextSocketStreamer} API.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ignite.{sh|bat} examples/config/example-cache.xml'}.
 * <p>
 * Alternatively you can run {@link CacheNodeStartup} in another JVM which will
 * start node with {@code examples/config/example-cache.xml} configuration.
 */
public class TextSocketStreamerExample {
    /** Cache name. */
    private static final String CACHE_NAME = "partitioned";

    /** Number of entries to load. */
    private static final int ENTRY_COUNT = 500000;

    /** Heap size required to run this example. */
    public static final int MIN_MEMORY = 512 * 1024 * 1024;

    /** Streaming server host. */
    private static final String HOST = "localhost";

    /** Streaming server port. */
    private static final int PORT = 5555;

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws IgniteException If example execution failed.
     */
    public static void main(String[] args) throws IgniteException {
        ExamplesUtils.checkMinMemory(MIN_MEMORY);

        try (Ignite ignite = Ignition.start("examples/config/example-cache.xml")) {
            System.out.println();
            System.out.println(">>> Cache data streamer example started.");

            startServer();

            // Clean up caches on all nodes before run.
            ignite.jcache(CACHE_NAME).clear();

            System.out.println();
            System.out.println(">>> Cache clear finished.");

            long start = System.currentTimeMillis();

            try (IgniteDataStreamer<Integer, String> stmr = ignite.dataStreamer(CACHE_NAME)) {
                // Configure loader.
                stmr.perNodeBufferSize(1024);
                stmr.perNodeParallelOperations(8);

                IgniteClosure<String, Map.Entry<Integer, String>> converter =
                    new IgniteClosure<String, Map.Entry<Integer, String>>() {
                        @Override public Map.Entry<Integer, String> apply(String input) {
                            String[] pair = input.split("=");
                            return new IgniteBiTuple<>(Integer.parseInt(pair[0]), pair[1]);
                        }
                };

                IgniteTextSocketStreamer<Integer, String> sockStmr =
                    new IgniteTextSocketStreamer<>(HOST, PORT, stmr, converter);

                IgniteFuture<Void> fut = sockStmr.start();

                try {
                    fut.get(500);
                } catch (IgniteFutureTimeoutException e) {
                    // No-op.
                }

                //fut.get();

                sockStmr.stop();

                System.out.println(">>> Future done: " + fut.isDone());
                System.out.println(">>> Future canceled: " + fut.isCancelled());
            }

            long end = System.currentTimeMillis();

            System.out.println(">>> Cache Size " + ignite.jcache(CACHE_NAME).size(CachePeekMode.PRIMARY));

            System.out.println(">>> Loaded " + ENTRY_COUNT + " keys in " + (end - start) + "ms.");
        }
    }

    /**
     * Starts streaming server and writes data into socket.
     */
    private static void startServer() {
        new Thread() {
            @Override public void run() {
                System.out.println();
                System.out.println(">>> Streaming server thread is started.");

                try (ServerSocket srvSock = new ServerSocket(PORT);
                     Socket sock = srvSock.accept();
                     BufferedWriter writer =
                         new BufferedWriter(new OutputStreamWriter(sock.getOutputStream(), "UTF-8"))) {

                    for (int i = 0; i < ENTRY_COUNT; i++) {
                        String num = Integer.toString(i);

                        writer.write(num + '=' + num);

                        writer.newLine();
                    }
                }
                catch (IOException e) {
                    // No-op.
                }

                System.out.println();
                System.out.println(">>> Streaming server thread is finished.");
            }
        }.start();
    }
}
