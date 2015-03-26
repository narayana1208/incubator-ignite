package org.apache.ignite.streaming;

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.testframework.junits.common.*;

import java.io.*;
import java.net.*;
import java.util.*;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 * Test for data loading using {@link IgniteTextSocketStreamer}.
 */
public class IgniteTextSocketStreamerTest extends GridCommonAbstractTest {
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
    public void testStream() throws Exception {
        try (Ignite g = startGrid()) {

            IgniteCache<Integer, String> cache = g.jcache(null);

            cache.clear();

            try (IgniteDataStreamer<Integer, String> stmr = g.dataStreamer(null)) {

                startServer();

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

                fut.get();

                System.out.println(">>> STATE " + sockStmr.state());

                assertTrue(fut.isDone());
                assertFalse(fut.isCancelled());
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
                     BufferedWriter writer =
                         new BufferedWriter(new OutputStreamWriter(sock.getOutputStream(), "UTF-8"))) {

                    for (int i = 0; i < ENTRY_CNT; i++) {
                        String num = Integer.toString(i);

                        writer.write(num + '=' + num);

                        writer.newLine();
                    }
                }
                catch (IOException e) {
                    // No-op.
                }
            }
        }.start();
    }
}