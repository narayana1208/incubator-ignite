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

package org.apache.ignite.spi.discovery.tcp.messages;

import java.io.*;
import java.util.*;

/**
 * Ping response.
 */
public class TcpDiscoveryPingResponse extends TcpDiscoveryAbstractMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** Whether pinged client exists. */
    private boolean clientExists;

    /**
     * For {@link Externalizable}.
     */
    public TcpDiscoveryPingResponse() {
        // No-op.
    }

    /**
     * @param creatorNodeId Creator node ID.
     */
    public TcpDiscoveryPingResponse(UUID creatorNodeId) {
        super(creatorNodeId);
    }

    /**
     * @param clientExists Whether pinged client exists.
     */
    public void clientExists(boolean clientExists) {
        this.clientExists = clientExists;
    }

    /**
     * @return Whether pinged client exists.
     */
    public boolean clientExists() {
        return clientExists;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);

        out.writeBoolean(clientExists);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);

        clientExists = in.readBoolean();
    }
}
