/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.metadata;

import com.facebook.drift.annotations.ThriftConstructor;
import com.facebook.drift.annotations.ThriftEnum;
import com.facebook.drift.annotations.ThriftEnumValue;
import com.facebook.drift.annotations.ThriftField;
import com.facebook.drift.annotations.ThriftStruct;
import com.facebook.presto.client.NodeVersion;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.NodePoolType;

import java.net.URI;
import java.util.OptionalInt;

import static com.facebook.presto.metadata.InternalNode.NodeStatus.ALIVE;
import static com.facebook.presto.spi.NodePoolType.DEFAULT;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Strings.emptyToNull;
import static com.google.common.base.Strings.nullToEmpty;
import static java.util.Objects.requireNonNull;

/**
 * A node is a server in a cluster than can process queries.
 */
@ThriftStruct
public class InternalNode
        implements Node
{
    @ThriftEnum
    public enum NodeStatus
    {
        ALIVE(1),
        DEAD(2),
        /**/;

        private final int statusCode;

        NodeStatus(int statusCode)
        {
            this.statusCode = statusCode;
        }

        @ThriftEnumValue
        public int getStatusCode()
        {
            return statusCode;
        }
    }

    private final String nodeIdentifier;
    private final URI internalUri;
    private final OptionalInt thriftPort;
    private final NodeVersion nodeVersion;
    private final boolean coordinator;
    private final boolean resourceManager;
    private final boolean catalogServer;
    private final NodeStatus nodeStatus;
    private final OptionalInt raftPort;
    private final NodePoolType poolType;
    private final int totalBins = 10;

    public InternalNode(String nodeIdentifier, URI internalUri, NodeVersion nodeVersion, boolean coordinator)
    {
        this(nodeIdentifier, internalUri, nodeVersion, coordinator, false, false);
    }

    public InternalNode(String nodeIdentifier, URI internalUri, NodeVersion nodeVersion, boolean coordinator, boolean resourceManager, boolean catalogServer)
    {
        this(nodeIdentifier, internalUri, OptionalInt.empty(), nodeVersion, coordinator, resourceManager, catalogServer, ALIVE, OptionalInt.empty(), DEFAULT);
    }

    @ThriftConstructor
    public InternalNode(String nodeIdentifier, URI internalUri, OptionalInt thriftPort, String nodeVersion, boolean coordinator, boolean resourceManager, boolean catalogServer)
    {
        this(nodeIdentifier, internalUri, thriftPort, new NodeVersion(nodeVersion), coordinator, resourceManager, catalogServer, ALIVE, OptionalInt.empty(), DEFAULT);
    }

    public InternalNode(
            String nodeIdentifier,
            URI internalUri,
            OptionalInt thriftPort,
            NodeVersion nodeVersion,
            boolean coordinator,
            boolean resourceManager,
            boolean catalogServer,
            NodeStatus nodeStatus,
            OptionalInt raftPort,
            NodePoolType poolType)
    {
        nodeIdentifier = emptyToNull(nullToEmpty(nodeIdentifier).trim());
        this.nodeIdentifier = requireNonNull(nodeIdentifier, "nodeIdentifier is null or empty");
        this.internalUri = requireNonNull(internalUri, "internalUri is null");
        this.thriftPort = requireNonNull(thriftPort, "thriftPort is null");
        this.nodeVersion = requireNonNull(nodeVersion, "nodeVersion is null");
        this.coordinator = coordinator;
        this.resourceManager = resourceManager;
        this.catalogServer = catalogServer;
        this.nodeStatus = nodeStatus;
        this.raftPort = requireNonNull(raftPort, "raftPort is null");
        this.poolType = requireNonNull(poolType, "poolType is null");
    }

    @ThriftField(1)
    @Override
    public String getNodeIdentifier()
    {
        return nodeIdentifier;
    }

    @Override
    public String getHost()
    {
        return internalUri.getHost();
    }

    @Override
    @Deprecated
    public URI getHttpUri()
    {
        return getInternalUri();
    }

    @ThriftField(3)
    public OptionalInt getThriftPort()
    {
        return thriftPort;
    }

    @ThriftField(value = 2, name = "internalUri")
    public URI getInternalUri()
    {
        return internalUri;
    }

    @Override
    public HostAddress getHostAndPort()
    {
        return HostAddress.fromUri(internalUri);
    }

    @ThriftField(value = 4, name = "nodeVersion")
    @Override
    public String getVersion()
    {
        return nodeVersion.getVersion();
    }

    @ThriftField(5)
    @Override
    public boolean isCoordinator()
    {
        return coordinator;
    }

    @ThriftField(6)
    @Override
    public boolean isResourceManager()
    {
        return resourceManager;
    }

    public NodeVersion getNodeVersion()
    {
        return nodeVersion;
    }

    @ThriftField(7)
    public NodeStatus getNodeStatus()
    {
        return nodeStatus;
    }

    @ThriftField(8)
    @Override
    public boolean isCatalogServer()
    {
        return catalogServer;
    }

    public OptionalInt getRaftPort()
    {
        return raftPort;
    }

    @Override
    public NodePoolType getPoolType()
    {
        return poolType;
    }

    @Override
    public int getVirtualBin()
    {
        if (poolType != DEFAULT) {
            switch (poolType) {
                case BIN00:
                    return 0;
                case BIN01:
                    return 1;
                case BIN02:
                    return 2;
                case BIN03:
                    return 3;
                case BIN04:
                    return 4;
                case BIN05:
                    return 5;
                case BIN06:
                    return 6;
                case BIN07:
                    return 7;
                case BIN08:
                    return 8;
                case BIN09:
                    return 9;
                default:
                    throw new UnsupportedOperationException();
            }
        }
        else {
            return (getInternalUri().hashCode() & 0xfffffff) % totalBins;
        }
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        InternalNode o = (InternalNode) obj;
        return nodeIdentifier.equals(o.nodeIdentifier);
    }

    @Override
    public int hashCode()
    {
        return nodeIdentifier.hashCode();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("nodeIdentifier", nodeIdentifier)
                .add("internalUri", internalUri)
                .add("thriftPort", thriftPort)
                .add("nodeVersion", nodeVersion)
                .add("coordinator", coordinator)
                .add("resourceManager", resourceManager)
                .add("catalogServer", catalogServer)
                .add("raftPort", raftPort)
                .add("poolType", poolType)
                .toString();
    }
}
