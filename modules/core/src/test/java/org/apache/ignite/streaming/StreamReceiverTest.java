package org.apache.ignite.streaming;

import junit.framework.TestCase;
import org.apache.ignite.*;
import org.apache.ignite.lang.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 *
 */
public class StreamReceiverTest extends TestCase {

    private static final IgniteClosure<Integer, Map.Entry<Integer, String>> CONVERTER =
        new IgniteClosure<Integer, Map.Entry<Integer, String>>() {
        @Override public Map.Entry<Integer, String> apply(Integer input) {
            return new IgniteBiTuple<>(input, input.toString());
        }
    };

    private static final IgniteDataStreamer<Integer, String> STMR = new DataStreamerStub<>();

    private volatile boolean finished = false;

    public void testName() throws Exception {
        StreamReceiver<Integer, Integer, String> receiver =
            new StreamReceiver<Integer, Integer, String>(STMR, CONVERTER) {
                @Override protected void loadData() {
                    while (!isStopped() && !finished) {
                        try {
                            Thread.sleep(50);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }
            };

        assertEquals(StreamReceiver.State.INITIALIZED, receiver.state());
        assertFalse(receiver.isStarted());
        assertFalse(receiver.isStopped());

        IgniteFuture<Void> fut = receiver.start();

        assertEquals(StreamReceiver.State.STARTED, receiver.state());

        assertTrue(receiver.isStarted());
        assertFalse(receiver.isStopped());

        assertFalse(fut.isDone());
        assertFalse(fut.isCancelled());

        try {
            fut.get(500);
        }
        catch (IgniteException e) {
            // No-op.
        }

        assertEquals(StreamReceiver.State.STARTED, receiver.state());
        assertTrue(receiver.isStarted());
        assertFalse(receiver.isStopped());

        assertFalse(fut.isDone());
        assertFalse(fut.isCancelled());

        //finished = true;
        receiver.stop();

        fut.get();

        assertEquals(StreamReceiver.State.STOPPED, receiver.state());

        assertFalse(receiver.isStarted());
        assertTrue(receiver.isStopped());

        assertTrue(fut.isDone());
        assertFalse(fut.isCancelled());

    }

    private static class DataStreamerStub<K, V> implements IgniteDataStreamer<K, V> {

        /** {@inheritDoc} */
        @Override public String cacheName() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public boolean allowOverwrite() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public void allowOverwrite(boolean allowOverwrite) throws IgniteException {

        }

        /** {@inheritDoc} */
        @Override public boolean skipStore() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public void skipStore(boolean skipStore) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public int perNodeBufferSize() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public void perNodeBufferSize(int bufSize) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public int perNodeParallelOperations() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public void perNodeParallelOperations(int parallelOps) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public long autoFlushFrequency() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public void autoFlushFrequency(long autoFlushFreq) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public IgniteFuture<?> future() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void deployClass(Class<?> depCls) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void updater(Updater<K, V> updater) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public IgniteFuture<?> removeData(K key) throws IgniteException, IllegalStateException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public IgniteFuture<?> addData(K key, @Nullable V val) throws IgniteException, IllegalStateException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public IgniteFuture<?> addData(Map.Entry<K, V> entry) throws IgniteException, IllegalStateException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public IgniteFuture<?> addData(Collection<? extends Map.Entry<K, V>> entries)
            throws IllegalStateException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public IgniteFuture<?> addData(Map<K, V> entries) throws IllegalStateException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void flush() throws IgniteException, IllegalStateException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void tryFlush() throws IgniteException, IllegalStateException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void close(boolean cancel) throws IgniteException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void close() throws IgniteException {
            // No-op.
        }
    }
}