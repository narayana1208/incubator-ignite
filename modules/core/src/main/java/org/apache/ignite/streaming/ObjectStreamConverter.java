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
import org.apache.ignite.lang.*;

import java.io.*;
import java.util.*;

/**
 *
 */
public class ObjectStreamConverter<K, V> implements IgniteClosure<InputStream, Iterator<Map.Entry<K, V>>> {
    /** Key closure. */
    private final IgniteClosure<V, K> keyClos;

    public ObjectStreamConverter(IgniteClosure<V, K> keyClos) {
        this.keyClos = keyClos;
    }

    /** {@inheritDoc} */
    @Override public Iterator<Map.Entry<K, V>> apply(final InputStream is) {

        final IgniteSocketStreamer.NextIterator<V> it;

        try {
            it = new NextObjectIterator<>(is);
        }
        catch (IOException e) {
            throw new IgniteException(e);
        }

        return new Iterator<Map.Entry<K, V>>() {
            @Override public boolean hasNext() {
                return it.hasNext();
            }

            @Override public Map.Entry<K, V> next() {
                V next = it.next();

                return new IgniteBiTuple<>(keyClos.apply(next), next);
            }

            @Override public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    private static class NextObjectIterator<E> extends IgniteSocketStreamer.NextIterator<E> {
        /** Object input stream. */
        private final ObjectInputStream ois;

        public NextObjectIterator(InputStream is) throws IOException {
            this.ois = new ObjectInputStream(new BufferedInputStream(is));
        }

        /** {@inheritDoc} */
        @SuppressWarnings("unchecked")
        @Override protected E getNext() {
            try {
                E obj = null;

                try {
                    obj = (E) ois.readObject();
                } catch (EOFException e) {
                    close();
                }

                return obj;
            } catch (IOException | ClassNotFoundException e) {
                throw new IgniteException(e);
            }
        }
    }
}
