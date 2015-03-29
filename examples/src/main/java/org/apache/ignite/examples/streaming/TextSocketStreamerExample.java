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
import org.apache.ignite.examples.ExampleNodeStartup;
import org.apache.ignite.examples.ExamplesUtils;
import org.apache.ignite.examples.streaming.numbers.CacheConfig;
import org.apache.ignite.examples.streaming.numbers.QueryPopularNumbers;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.streaming.IgniteTextSocketStreamer;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;
import java.util.Random;

/**
 * Stream random numbers into the streaming cache.
 * To start the example, you should:
 * <ul>
 *     <li>Start a few nodes using {@link ExampleNodeStartup} or by starting remote nodes as specified below.</li>
 *     <li>Start querying popular numbers using {@link QueryPopularNumbers}.</li>
 *     <li>Start streaming using {@link TextSocketStreamerExample}.</li>
 * </ul>
 * <p>
 * You should start remote nodes by running {@link ExampleNodeStartup} in another JVM.
 */
public class TextSocketStreamerExample {
    /** Random number generator. */
    private static final Random RAND = new Random();

    /** Range within which to generate numbers. */
    private static final int RANGE = 1000;

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
    public static void main(String[] args) throws IgniteException, InterruptedException {
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            if (!ExamplesUtils.hasServerNodes(ignite))
                return;

            startServer();

            // The cache is configured with sliding window holding 1 second of the streaming data.
            IgniteCache<Integer, Long> stmCache = ignite.getOrCreateCache(CacheConfig.randomNumbersCache());

            try (IgniteDataStreamer<Integer, Long> stmr = ignite.dataStreamer(stmCache.getName())) {
                // Allow data updates.
                stmr.allowOverwrite(true);

                IgniteClosure<String, Map.Entry<Integer, Long>> converter =
                    new IgniteClosure<String, Map.Entry<Integer, Long>>() {
                        @Override public Map.Entry<Integer, Long> apply(String input) {
                            String[] pair = input.split("=");
                            return new IgniteBiTuple<>(Integer.parseInt(pair[0]), Long.parseLong(pair[1]));
                        }
                };

                IgniteTextSocketStreamer<Integer, Long> sockStmr =
                    new IgniteTextSocketStreamer<>(HOST, PORT, stmr, converter);

                sockStmr.start();

                while(true)
                    Thread.sleep(1000);
            }
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


                    while(true) {
                        int key = RAND.nextInt(RANGE);

                        int value = RAND.nextInt(RANGE) + 1;

                        writer.write(Integer.toString(key) + '=' + Integer.toString(value));

                        writer.newLine();

                        try {
                            Thread.sleep(1);
                        }
                        catch (InterruptedException e) {
                            // No-op.
                        }
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
