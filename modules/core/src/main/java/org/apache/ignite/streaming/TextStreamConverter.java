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

public class TextStreamConverter implements IgniteClosure<InputStream, Iterator<Map.Entry<String, String>>> {
    /** {@inheritDoc} */
    @Override public Iterator<Map.Entry<String, String>> apply(final InputStream is) {

        final IgniteSocketStreamer.NextIterator<String> it;

        try {
            it = new NextStringIterator(is);
        }
        catch (UnsupportedEncodingException e) {
            throw new IgniteException(e);
        }

        return new Iterator<Map.Entry<String, String>>() {
            @Override public boolean hasNext() {
                return it.hasNext();
            }

            @Override public Map.Entry<String, String> next() {
                String[] pair = it.next().split("=");

                return new IgniteBiTuple<>(pair[0], pair[1]);
            }

            @Override public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    private static class NextStringIterator extends IgniteSocketStreamer.NextIterator<String> {
        /** Buffered reader. */
        private final BufferedReader reader;

        public NextStringIterator(InputStream is) throws UnsupportedEncodingException {
            this.reader = new BufferedReader(new InputStreamReader(is, "UTF-8"));
        }

        /** {@inheritDoc} */
        @Override protected String getNext() {
            try {
                String val = reader.readLine();

                if (val == null)
                    close();

                return val;
            }
            catch (Exception e) {
                throw new IgniteException(e);
            }
        }
    }
}
