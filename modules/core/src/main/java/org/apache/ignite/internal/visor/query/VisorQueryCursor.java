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

package org.apache.ignite.internal.visor.query;

import org.apache.ignite.cache.query.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.query.*;

import java.util.*;

/**
 * Wrapper for query cursor.
 */
public class VisorQueryCursor<T> implements Iterator<T>, AutoCloseable {
    /** */
    private final QueryCursor<T> cur;

    /** */
    private final Iterator<T> itr;

    /**
     * @param cur Cursor.
     */
    public VisorQueryCursor(QueryCursor<T> cur) {
        this.cur = cur;

        itr = cur.iterator();
    }

    /** {@inheritDoc} */
    @Override public boolean hasNext() {
        return itr.hasNext();
    }

    /** {@inheritDoc} */
    @Override public T next() {
        return itr.next();
    }

    /** {@inheritDoc} */
    @Override public void remove() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void close() throws Exception {
        cur.close();
    }

    /**
     * @return SQL Fields query result metadata.
     */
    @SuppressWarnings("unchecked")
    public Collection<GridQueryFieldMetadata> fieldsMeta() {
        return ((QueryCursorImpl)cur).fieldsMeta();
    }
}