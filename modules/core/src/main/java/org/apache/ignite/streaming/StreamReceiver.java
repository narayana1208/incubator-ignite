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

import java.util.*;
import java.util.concurrent.*;

/**
 * Base implementation of stream receiver.
 *
 * @param <E> Type of stream element.
 * @param <K> Type of cache entry key.
 * @param <V> Type of cache entry value/
 */
public abstract class StreamReceiver<E, K, V> {
    /** Object monitor. */
    private final Object lock = new Object();

    /** Stop latch. */
    private final CountDownLatch stopLatch = new CountDownLatch(1);

    /** State. */
    private volatile State state = State.INITIALIZED;

    /** Target streamer. */
    private final IgniteDataStreamer<K, V> streamer;

    /** Element to entries transformer. */
    private final IgniteClosure<E, Map.Entry<K, V>> converter;

    /**
     * Constructs stream receiver.
     *
     * @param streamer Streamer.
     * @param converter Element to entries transformer.
     */
    public StreamReceiver(IgniteDataStreamer<K, V> streamer, IgniteClosure<E, Map.Entry<K, V>> converter) {
        A.notNull(streamer, "streamer is null");
        A.notNull(converter, "converter is null");

        this.streamer = streamer;
        this.converter = converter;
    }

    /**
     * Starts streamer.
     */
    public IgniteFuture<Void> start() {
        synchronized (lock) {
            if (state != State.INITIALIZED)
                throw new IllegalStateException("Receiver in " + state + " state can't be started.");

            GridFutureAdapter<Void> fut = new GridFutureAdapter<>();

            new Thread(new Receiver(fut)).start();

            state = State.STARTED;

            return new IgniteFutureImpl<>(fut);
        }
    }

    /**
     * Stops streamer.
     */
    public void stop() {
        synchronized (lock) {
            if (state != State.STARTED)
                throw new IllegalStateException("Receiver in " + state + " state can't be stopped.");

            state = State.STOPPED;

            try {
                stopLatch.await();
            }
            catch (InterruptedException e) {
                // No-op.
            }
        }
    }

    /**
     * Returns stream receiver state.
     *
     * @return stream receiver state.
     */
    public State state() {
        return state;
    }

    /**
     * Checks whether receiver is started or not.
     *
     * @return {@code True} if receiver is started, {@code false} - otherwise.
     */
    public boolean isStarted() {
        return state == State.STARTED;
    }

    /**
     * Checks whether receiver is stopped or not.
     *
     * @return {@code True} if receiver is stopped, {@code false} - otherwise.
     */
    public boolean isStopped() {
        return state == State.STOPPED;
    }

    /**
     * Performs actual loading of data. Override this method in order to implement own data loading functionality.
     */
    protected abstract void loadData();

    /**
     * Convert stream data to cache entry and transfer it to the target streamer.
     *
     * @param element Element.
     */
    protected void addData(E element) {
        streamer.addData(converter.apply(element));
    }

    /**
     * Receiver state.
     */
    public enum State {
        /** New. */
        INITIALIZED,
        /** Started. */
        STARTED,
        /** Stopped. */
        STOPPED
    }

    /**
     * Receiver worker that actually receives data from socket.
     */
    private class Receiver implements Runnable {
        /** Future. */
        private final GridFutureAdapter<Void> fut;

        /**
         * @param fut Future.
         */
        public Receiver(GridFutureAdapter<Void> fut) {
            this.fut = fut;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            Throwable err = null;

            try {
                loadData();
            }
            catch (Throwable e) {
                err = e;
            }
            finally {
                if (state == State.STOPPED)
                    fut.onCancelled();
                else {
                    state = State.STOPPED;

                    fut.onDone(null, err);
                }

                stopLatch.countDown();
            }
        }
    }
}
