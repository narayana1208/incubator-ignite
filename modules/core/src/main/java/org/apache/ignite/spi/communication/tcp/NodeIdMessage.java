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

package org.apache.ignite.spi.communication.tcp;

import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.plugin.extensions.communication.*;

import java.nio.*;
import java.util.*;

/**
 * Message with node ID.
 */
public class NodeIdMessage implements Message {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private byte[] nodeIdBytes;

    /** */
    private byte[] nodeIdBytesWithType;

    /** */
    public NodeIdMessage() {
        // No-op.
    }

    /**
     * @param nodeId Node ID.
     */
    public NodeIdMessage(UUID nodeId) {
        nodeIdBytes = U.uuidToBytes(nodeId);

        nodeIdBytesWithType = new byte[nodeIdBytes.length + 1];

        nodeIdBytesWithType[0] = TcpCommunicationSpi.NODE_ID_MSG_TYPE;

        System.arraycopy(nodeIdBytes, 0, nodeIdBytesWithType, 1, nodeIdBytes.length);
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        assert nodeIdBytes.length == 16;

        if (buf.remaining() < 17)
            return false;

        buf.put(TcpCommunicationSpi.NODE_ID_MSG_TYPE);
        buf.put(nodeIdBytes);

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        if (buf.remaining() < 16)
            return false;

        nodeIdBytes = new byte[16];

        buf.get(nodeIdBytes);

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return TcpCommunicationSpi.NODE_ID_MSG_TYPE;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 0;
    }

    /**
     * @return Node id bytes with type.
     */
    public byte[] nodeIdBytesWithType() {
        return nodeIdBytesWithType;
    }

    /**
     * @return Node id bytes.
     */
    public byte[] nodeIdBytes() {
        return nodeIdBytes;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(NodeIdMessage.class, this);
    }
}
