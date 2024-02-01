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
package com.facebook.presto.execution.scheduler;

import com.facebook.airlift.stats.CounterStat;
import com.facebook.presto.Session;
import com.facebook.presto.execution.NodeTaskMap;
import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.execution.RemoteTask;
import com.facebook.presto.execution.scheduler.nodeSelection.NodeSelectionStats;
import com.facebook.presto.execution.scheduler.nodeSelection.NodeSelector;
import com.facebook.presto.execution.scheduler.nodeSelection.SimpleNodeSelector;
import com.facebook.presto.execution.scheduler.nodeSelection.SimpleTtlNodeSelector;
import com.facebook.presto.execution.scheduler.nodeSelection.SimpleTtlNodeSelectorConfig;
import com.facebook.presto.execution.scheduler.nodeSelection.TopologyAwareNodeSelector;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.SplitContext;
import com.facebook.presto.spi.SplitWeight;
import com.facebook.presto.ttl.nodettlfetchermanagers.NodeTtlFetcherManager;
import com.google.common.base.Supplier;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.Duration;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;

import static com.facebook.airlift.concurrent.MoreFutures.whenAnyCompleteCancelOthers;
import static com.facebook.presto.SystemSessionProperties.getMaxUnacknowledgedSplitsPerTask;
import static com.facebook.presto.SystemSessionProperties.getResourceAwareSchedulingStrategy;
import static com.facebook.presto.execution.scheduler.NodeSchedulerConfig.NetworkTopologyType;
import static com.facebook.presto.execution.scheduler.NodeSchedulerConfig.ResourceAwareSchedulingStrategy;
import static com.facebook.presto.execution.scheduler.NodeSchedulerConfig.ResourceAwareSchedulingStrategy.TTL;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Suppliers.memoizeWithExpiration;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static java.lang.Math.addExact;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class NodeScheduler
{
    private final NetworkLocationCache networkLocationCache;
    private final List<CounterStat> topologicalSplitCounters;
    private final List<String> networkLocationSegmentNames;
    private final InternalNodeManager nodeManager;
    private final NodeSelectionStats nodeSelectionStats;
    private final int minCandidates;
    private final boolean includeCoordinator;
    private final long maxSplitsWeightPerNode;
    private final long maxPendingSplitsWeightPerTask;
    private final NodeTaskMap nodeTaskMap;
    private final boolean useNetworkTopology;
    private final Duration nodeSetRefreshInterval;
    private final NodeTtlFetcherManager nodeTtlFetcherManager;
    private final QueryManager queryManager;
    private final SimpleTtlNodeSelectorConfig simpleTtlNodeSelectorConfig;
    private final NodeSelectionHashStrategy nodeSelectionHashStrategy;
    private final int minVirtualNodeCount;
    private final NodeSetSupplier nodeSetSupplier;

    @Inject
    public NodeScheduler(
            NetworkTopology networkTopology,
            InternalNodeManager nodeManager,
            NodeSelectionStats nodeSelectionStats,
            NodeSchedulerConfig config,
            NodeTaskMap nodeTaskMap,
            NodeTtlFetcherManager nodeTtlFetcherManager,
            QueryManager queryManager,
            SimpleTtlNodeSelectorConfig simpleTtlNodeSelectorConfig,
            NodeSetSupplier nodeSetSupplier)
    {
        this(new NetworkLocationCache(networkTopology),
                networkTopology,
                nodeManager,
                nodeSelectionStats,
                config,
                nodeTaskMap,
                new Duration(5, SECONDS),
                nodeTtlFetcherManager,
                queryManager,
                simpleTtlNodeSelectorConfig,
                nodeSetSupplier);
    }

    public NodeScheduler(
            NetworkLocationCache networkLocationCache,
            NetworkTopology networkTopology,
            InternalNodeManager nodeManager,
            NodeSelectionStats nodeSelectionStats,
            NodeSchedulerConfig config,
            NodeTaskMap nodeTaskMap,
            Duration nodeSetRefreshInterval,
            NodeTtlFetcherManager nodeTtlFetcherManager,
            QueryManager queryManager,
            SimpleTtlNodeSelectorConfig simpleTtlNodeSelectorConfig,
            NodeSetSupplier nodeSetSupplier)
    {
        this.networkLocationCache = networkLocationCache;
        this.nodeManager = nodeManager;
        this.nodeSelectionStats = requireNonNull(nodeSelectionStats, "nodeSelectionStats is null");
        this.minCandidates = config.getMinCandidates();
        this.includeCoordinator = config.isIncludeCoordinator();
        int maxSplitsPerNode = config.getMaxSplitsPerNode();
        int maxPendingSplitsPerTask = config.getMaxPendingSplitsPerTask();
        checkArgument(maxSplitsPerNode >= maxPendingSplitsPerTask, "maxSplitsPerNode must be > maxPendingSplitsPerTask");
        this.maxSplitsWeightPerNode = SplitWeight.rawValueForStandardSplitCount(maxSplitsPerNode);
        this.maxPendingSplitsWeightPerTask = SplitWeight.rawValueForStandardSplitCount(maxPendingSplitsPerTask);
        this.nodeTaskMap = requireNonNull(nodeTaskMap, "nodeTaskMap is null");
        this.useNetworkTopology = !config.getNetworkTopology().equals(NetworkTopologyType.LEGACY);

        ImmutableList.Builder<CounterStat> builder = ImmutableList.builder();
        if (useNetworkTopology) {
            networkLocationSegmentNames = ImmutableList.copyOf(networkTopology.getLocationSegmentNames());
            for (int i = 0; i < networkLocationSegmentNames.size() + 1; i++) {
                builder.add(new CounterStat());
            }
        }
        else {
            networkLocationSegmentNames = ImmutableList.of();
        }
        topologicalSplitCounters = builder.build();
        this.nodeSetRefreshInterval = requireNonNull(nodeSetRefreshInterval, "nodeSetRefreshInterval is null");
        this.nodeTtlFetcherManager = requireNonNull(nodeTtlFetcherManager, "nodeTtlFetcherManager is null");
        this.queryManager = requireNonNull(queryManager, "queryManager is null");
        this.simpleTtlNodeSelectorConfig = requireNonNull(simpleTtlNodeSelectorConfig, "simpleTtlNodeSelectorConfig is null");
        this.nodeSelectionHashStrategy = config.getNodeSelectionHashStrategy();
        this.minVirtualNodeCount = config.getMinVirtualNodeCount();
        this.nodeSetSupplier = nodeSetSupplier;
    }

    @PreDestroy
    public void stop()
    {
        networkLocationCache.stop();
    }

    public Map<String, CounterStat> getTopologicalSplitCounters()
    {
        ImmutableMap.Builder<String, CounterStat> counters = ImmutableMap.builder();
        for (int i = 0; i < topologicalSplitCounters.size(); i++) {
            counters.put(i == 0 ? "all" : networkLocationSegmentNames.get(i - 1), topologicalSplitCounters.get(i));
        }
        return counters.build();
    }

    public NodeSelector createNodeSelector(Session session, ConnectorId connectorId)
    {
        return createNodeSelector(session, connectorId, Integer.MAX_VALUE, Optional.empty());
    }

    public NodeSelector createNodeSelector(Session session, ConnectorId connectorId, Optional<Predicate<Node>> filterNodePredicate)
    {
        return createNodeSelector(session, connectorId, Integer.MAX_VALUE, filterNodePredicate);
    }

    public NodeSelector createNodeSelector(Session session, ConnectorId connectorId, int maxTasksPerStage)
    {
        return createNodeSelector(session, connectorId, maxTasksPerStage, Optional.empty());
    }

    public NodeSelector createNodeSelector(Session session, ConnectorId connectorId, int maxTasksPerStage, Optional<Predicate<Node>> filterNodePredicate)
    {
        // this supplier is thread-safe. TODO: this logic should probably move to the scheduler since the choice of which node to run in should be
        // done as close to when the split is about to be scheduled
        Supplier<NodeSet> nodeSet = nodeSetSupplier.createNodeSetSupplierForQuery(
                session.getQueryId(),
                connectorId,
                filterNodePredicate,
                nodeSelectionHashStrategy,
                useNetworkTopology,
                minVirtualNodeCount,
                includeCoordinator);

        nodeSet = nodeSetRefreshInterval.toMillis() > 0 ?
                memoizeWithExpiration(nodeSet, nodeSetRefreshInterval.toMillis(), MILLISECONDS) : nodeSet;

        int maxUnacknowledgedSplitsPerTask = getMaxUnacknowledgedSplitsPerTask(requireNonNull(session, "session is null"));
        ResourceAwareSchedulingStrategy resourceAwareSchedulingStrategy = getResourceAwareSchedulingStrategy(session);
        if (useNetworkTopology) {
            return new TopologyAwareNodeSelector(
                    nodeManager,
                    nodeSelectionStats,
                    nodeTaskMap,
                    includeCoordinator,
                    nodeSet,
                    minCandidates,
                    maxSplitsWeightPerNode,
                    maxPendingSplitsWeightPerTask,
                    maxUnacknowledgedSplitsPerTask,
                    topologicalSplitCounters,
                    networkLocationSegmentNames,
                    networkLocationCache,
                    nodeSelectionHashStrategy);
        }

        SimpleNodeSelector simpleNodeSelector = new SimpleNodeSelector(
                nodeManager,
                nodeSelectionStats,
                nodeTaskMap,
                includeCoordinator,
                nodeSet,
                minCandidates,
                maxSplitsWeightPerNode,
                maxPendingSplitsWeightPerTask,
                maxUnacknowledgedSplitsPerTask,
                maxTasksPerStage,
                nodeSelectionHashStrategy);

        if (resourceAwareSchedulingStrategy == TTL) {
            return new SimpleTtlNodeSelector(
                    simpleNodeSelector,
                    simpleTtlNodeSelectorConfig,
                    nodeTaskMap,
                    nodeSet,
                    minCandidates,
                    includeCoordinator,
                    maxSplitsWeightPerNode,
                    maxPendingSplitsWeightPerTask,
                    maxTasksPerStage,
                    nodeTtlFetcherManager,
                    queryManager,
                    session);
        }

        return simpleNodeSelector;
    }

    public CompletableFuture<?> acquireNodes(QueryId queryId, int count)
    {
        return nodeSetSupplier.acquireNodes(queryId, count);
    }

    public CompletableFuture<?> releaseNodes(QueryId queryId, int count)
    {
        return nodeSetSupplier.releaseNodes(queryId, count);
    }

    public static List<InternalNode> selectNodes(int limit, ResettableRandomizedIterator<InternalNode> candidates)
    {
        checkArgument(limit > 0, "limit must be at least 1");

        ImmutableList.Builder<InternalNode> selectedNodes = ImmutableList.builderWithExpectedSize(min(limit, candidates.size()));
        int selectedCount = 0;
        while (selectedCount < limit && candidates.hasNext()) {
            selectedNodes.add(candidates.next());
            selectedCount++;
        }

        return selectedNodes.build();
    }

    public static ResettableRandomizedIterator<InternalNode> randomizedNodes(NodeSet nodeSet, boolean includeCoordinator, Set<InternalNode> excludedNodes)
    {
        ImmutableList<InternalNode> nodes = nodeSet.getActiveNodes().stream()
                .filter(node -> includeCoordinator || !nodeSet.getCoordinatorNodeIds().contains(node.getNodeIdentifier()))
                .filter(node -> !excludedNodes.contains(node))
                .collect(toImmutableList());
        return new ResettableRandomizedIterator<>(nodes);
    }

    public static List<InternalNode> selectExactNodes(NodeSet nodeSet, List<HostAddress> hosts, boolean includeCoordinator)
    {
        Set<InternalNode> chosen = new LinkedHashSet<>();
        Set<String> coordinatorIds = nodeSet.getCoordinatorNodeIds();

        for (HostAddress host : hosts) {
            nodeSet.getAllNodesByHostAndPort().get(host).stream()
                    .filter(node -> includeCoordinator || !coordinatorIds.contains(node.getNodeIdentifier()))
                    .forEach(chosen::add);

            // consider a split with a host without a port as being accessible by all nodes in that host
            if (!host.hasPort()) {
                InetAddress address;
                try {
                    address = host.toInetAddress();
                }
                catch (UnknownHostException e) {
                    // skip hosts that don't resolve
                    continue;
                }

                nodeSet.getAllNodesByHost().get(address).stream()
                        .filter(node -> includeCoordinator || !coordinatorIds.contains(node.getNodeIdentifier()))
                        .forEach(chosen::add);
            }
        }

        // if the chosen set is empty and the host is the coordinator, force pick the coordinator
        if (chosen.isEmpty() && !includeCoordinator) {
            for (HostAddress host : hosts) {
                // In the code below, before calling `chosen::add`, it could have been checked that
                // `coordinatorIds.contains(node.getNodeIdentifier())`. But checking the condition isn't necessary
                // because every node satisfies it. Otherwise, `chosen` wouldn't have been empty.

                chosen.addAll(nodeSet.getAllNodesByHostAndPort().get(host));

                // consider a split with a host without a port as being accessible by all nodes in that host
                if (!host.hasPort()) {
                    InetAddress address;
                    try {
                        address = host.toInetAddress();
                    }
                    catch (UnknownHostException e) {
                        // skip hosts that don't resolve
                        continue;
                    }

                    chosen.addAll(nodeSet.getAllNodesByHost().get(address));
                }
            }
        }

        return ImmutableList.copyOf(chosen);
    }

    public static SplitPlacementResult selectDistributionNodes(
            NodeSet nodeSet,
            NodeTaskMap nodeTaskMap,
            long maxSplitsWeightPerNode,
            long maxPendingSplitsWeightPerTask,
            int maxUnacknowledgedSplitsPerTask,
            Set<Split> splits,
            List<RemoteTask> existingTasks,
            BucketNodeMap bucketNodeMap,
            NodeSelectionStats nodeSelectionStats)
    {
        Multimap<InternalNode, Split> assignments = HashMultimap.create();
        NodeAssignmentStats assignmentStats = new NodeAssignmentStats(nodeTaskMap, nodeSet, existingTasks);

        Set<InternalNode> blockedNodes = new HashSet<>();
        for (Split split : splits) {
            // node placement is forced by the bucket to node map
            InternalNode node = bucketNodeMap.getAssignedNode(split).get();
            boolean isCacheable = bucketNodeMap.isSplitCacheable(split);
            SplitWeight splitWeight = split.getSplitWeight();

            // if node is full, don't schedule now, which will push back on the scheduling of splits
            if (canAssignSplitToDistributionNode(assignmentStats, node, maxSplitsWeightPerNode, maxPendingSplitsWeightPerTask, maxUnacknowledgedSplitsPerTask, splitWeight)) {
                if (isCacheable) {
                    split = new Split(split.getConnectorId(), split.getTransactionHandle(), split.getConnectorSplit(), split.getLifespan(), new SplitContext(true));
                    nodeSelectionStats.incrementBucketedPreferredNodeSelectedCount();
                }
                else {
                    nodeSelectionStats.incrementBucketedNonPreferredNodeSelectedCount();
                }
                assignments.put(node, split);
                assignmentStats.addAssignedSplit(node, splitWeight);
            }
            else {
                blockedNodes.add(node);
            }
        }

        ListenableFuture<?> blocked = toWhenHasSplitQueueSpaceFuture(blockedNodes, existingTasks, calculateLowWatermark(maxPendingSplitsWeightPerTask));
        return new SplitPlacementResult(blocked, ImmutableMultimap.copyOf(assignments));
    }

    private static boolean canAssignSplitToDistributionNode(NodeAssignmentStats assignmentStats, InternalNode node, long maxSplitsWeightPerNode, long maxPendingSplitsWeightPerTask, int maxUnacknowledgedSplitsPerTask, SplitWeight splitWeight)
    {
        return assignmentStats.getUnacknowledgedSplitCountForStage(node) < maxUnacknowledgedSplitsPerTask &&
                (canAssignSplitBasedOnWeight(assignmentStats.getTotalSplitsWeight(node), maxSplitsWeightPerNode, splitWeight) ||
                        canAssignSplitBasedOnWeight(assignmentStats.getQueuedSplitsWeightForStage(node), maxPendingSplitsWeightPerTask, splitWeight));
    }

    public static boolean canAssignSplitBasedOnWeight(long currentWeight, long weightLimit, SplitWeight splitWeight)
    {
        // Nodes or tasks that are configured to accept any splits (ie: weightLimit > 0) should always accept at least one split when
        // empty (ie: currentWeight == 0) to ensure that forward progress can be made if split weights are huge
        return addExact(currentWeight, splitWeight.getRawValue()) <= weightLimit || (currentWeight == 0 && weightLimit > 0);
    }

    public static long calculateLowWatermark(long maxPendingSplitsWeightPerTask)
    {
        return (long) Math.ceil(maxPendingSplitsWeightPerTask * 0.5);
    }

    public static ListenableFuture<?> toWhenHasSplitQueueSpaceFuture(Set<InternalNode> blockedNodes, List<RemoteTask> existingTasks, long weightSpaceThreshold)
    {
        if (blockedNodes.isEmpty()) {
            return immediateFuture(null);
        }
        Map<String, RemoteTask> nodeToTaskMap = new HashMap<>();
        for (RemoteTask task : existingTasks) {
            nodeToTaskMap.put(task.getNodeId(), task);
        }
        List<ListenableFuture<?>> blockedFutures = blockedNodes.stream()
                .map(InternalNode::getNodeIdentifier)
                .map(nodeToTaskMap::get)
                .filter(Objects::nonNull)
                .map(remoteTask -> remoteTask.whenSplitQueueHasSpace(weightSpaceThreshold))
                .collect(toImmutableList());
        if (blockedFutures.isEmpty()) {
            return immediateFuture(null);
        }
        return whenAnyCompleteCancelOthers(blockedFutures);
    }

    public static ListenableFuture<?> toWhenHasSplitQueueSpaceFuture(List<RemoteTask> existingTasks, long weightSpaceThreshold)
    {
        if (existingTasks.isEmpty()) {
            return immediateFuture(null);
        }
        List<ListenableFuture<?>> stateChangeFutures = existingTasks.stream()
                .map(remoteTask -> remoteTask.whenSplitQueueHasSpace(weightSpaceThreshold))
                .collect(toImmutableList());
        return whenAnyCompleteCancelOthers(stateChangeFutures);
    }
}
