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
package com.facebook.presto.execution;

import com.facebook.presto.Session;
import com.facebook.presto.client.NodeVersion;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.cost.StatsAndCosts;
import com.facebook.presto.execution.scheduler.SplitSchedulerStats;
import com.facebook.presto.execution.scheduler.TableWriteInfo;
import com.facebook.presto.failureDetector.NoOpFailureDetector;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.operator.StageExecutionDescriptor;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.split.MockSplitSource;
import com.facebook.presto.sql.planner.Partitioning;
import com.facebook.presto.sql.planner.PartitioningScheme;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.plan.PlanFragmentId;
import com.facebook.presto.sql.planner.plan.RemoteSourceNode;
import com.facebook.presto.util.FinalizerService;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.UncheckedTimeoutException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.SystemSessionProperties.MAX_FAILED_TASK_PERCENTAGE;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.execution.SqlStageExecution.createSqlStageExecution;
import static com.facebook.presto.execution.buffer.OutputBuffers.BufferType.ARBITRARY;
import static com.facebook.presto.execution.buffer.OutputBuffers.createInitialEmptyOutputBuffers;
import static com.facebook.presto.spi.StandardErrorCode.HOST_SHUTTING_DOWN;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SOURCE_DISTRIBUTION;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.MoreCollectors.onlyElement;
import static com.google.common.util.concurrent.Uninterruptibles.awaitUninterruptibly;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

public class TestSqlStageExecution
{
    private ExecutorService executor;
    private ScheduledExecutorService scheduledExecutor;

    @BeforeClass
    public void setUp()
    {
        executor = newCachedThreadPool(daemonThreadsNamed("test-executor-%s"));
        scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed("test-scheduledExecutor-%s"));
    }

    @AfterClass
    public void tearDown()
    {
        executor.shutdownNow();
        executor = null;
        scheduledExecutor.shutdownNow();
        scheduledExecutor = null;
    }

    @Test(timeOut = 3 * 60 * 1000)
    public void testFinalStageInfo()
            throws Exception
    {
        // run test a few times to catch any race conditions
        // this is not done with TestNG invocation count so there can be a global time limit on the test
        for (int iteration = 0; iteration < 10; iteration++) {
            testFinalStageInfoInternal();
        }
    }

    private void testFinalStageInfoInternal()
            throws Exception
    {
        NodeTaskMap nodeTaskMap = new NodeTaskMap(new FinalizerService());

        StageId stageId = new StageId(new QueryId("query"), 0);
        SqlStageExecution stage = createSqlStageExecution(
                new StageExecutionId(stageId, 0),
                createExchangePlanFragment(),
                new MockRemoteTaskFactory(executor, scheduledExecutor),
                TEST_SESSION,
                true,
                nodeTaskMap,
                executor,
                new NoOpFailureDetector(),
                new SplitSchedulerStats(),
                new TableWriteInfo(Optional.empty(), Optional.empty(), Optional.empty()), false);
        stage.setOutputBuffers(createInitialEmptyOutputBuffers(ARBITRARY));

        // add listener that fetches stage info when the final status is available
        SettableFuture<StageExecutionInfo> finalStageInfo = SettableFuture.create();
        stage.addFinalStageInfoListener(finalStageInfo::set);

        // in a background thread add a ton of tasks
        CountDownLatch latch = new CountDownLatch(1000);
        Future<?> addTasksTask = executor.submit(() -> {
            try {
                for (int i = 0; i < 1_000_000; i++) {
                    if (Thread.interrupted()) {
                        return;
                    }
                    InternalNode node = new InternalNode(
                            "source" + i,
                            URI.create("http://10.0.0." + (i / 10_000) + ":" + (i % 10_000)),
                            NodeVersion.UNKNOWN,
                            false);
                    stage.scheduleTask(node, i);
                    latch.countDown();
                }
            }
            finally {
                while (latch.getCount() > 0) {
                    latch.countDown();
                }
            }
        });

        // wait for some tasks to be created, and then abort the query
        latch.await(1, MINUTES);
        assertFalse(stage.getStageExecutionInfo().getTasks().isEmpty());
        stage.abort();

        // once the final stage info is available, verify that it is complete
        StageExecutionInfo stageInfo = finalStageInfo.get(1, MINUTES);
        assertFalse(stageInfo.getTasks().isEmpty());
        assertTrue(stageInfo.isFinal());
        assertSame(stage.getStageExecutionInfo(), stageInfo);

        // cancel the background thread adding tasks
        addTasksTask.cancel(true);
    }

    private static PlanFragment createExchangePlanFragment()
    {
        PlanNode planNode = new RemoteSourceNode(
                Optional.empty(),
                new PlanNodeId("exchange"),
                ImmutableList.of(new PlanFragmentId(0)),
                ImmutableList.of(new VariableReferenceExpression(Optional.empty(), "column", VARCHAR)),
                false,
                Optional.empty(),
                REPARTITION);

        return new PlanFragment(
                new PlanFragmentId(0),
                planNode,
                ImmutableSet.copyOf(planNode.getOutputVariables()),
                SOURCE_DISTRIBUTION,
                ImmutableList.of(planNode.getId()),
                new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), planNode.getOutputVariables()),
                StageExecutionDescriptor.ungroupedExecution(),
                false,
                StatsAndCosts.empty(),
                Optional.empty());
    }
    @DataProvider(name = "noFailedTaskPlanNodeDataProvider")
    public Object[][] noFailedTaskPlanNodeDataProvider()
    {
        PlanNode tableScanNode = new TableScanNode(
                Optional.empty(),
                new PlanNodeId("exchange"),
                new TableHandle(new ConnectorId("test"), new ConnectorTableHandle() {}, new ConnectorTransactionHandle() {}, Optional.empty()),
                ImmutableList.of(),
                new HashMap<>(),
                TupleDomain.all(),
                TupleDomain.all());

        PlanNode remoteSourceNode = new RemoteSourceNode(
                Optional.empty(),
                new PlanNodeId("exchange"),
                ImmutableList.of(new PlanFragmentId(0)),
                ImmutableList.of(new VariableReferenceExpression(Optional.empty(), "column", VARCHAR)),
                false,
                Optional.empty(),
                REPARTITION);

        return new Object[][] {
                {tableScanNode, 0.3},
                {tableScanNode, 1.0},
                {remoteSourceNode, 0.3},
                {remoteSourceNode, 1.0},
        };
    }
    @Test(timeOut = 3 * 60 * 1000, dataProvider = "noFailedTaskPlanNodeDataProvider")
    public void testNoMoreRetryOnNoFailedTask(PlanNode planNode, double maxFailedTaskPercentage)
            throws Exception
    {
        // run test a few times to catch any race conditions
        // this is not done with TestNG invocation count so there can be a global time limit on the test
        for (int iteration = 0; iteration < 10; iteration++) {
            testNoMoreRetryOnNoFailedTaskInternal(planNode, maxFailedTaskPercentage);
        }
    }

    private void testNoMoreRetryOnNoFailedTaskInternal(PlanNode planNode, double maxFailedTaskPercentage)
            throws Exception
    {
        NodeTaskMap nodeTaskMap = new NodeTaskMap(new FinalizerService());

        PlanFragment plan = new PlanFragment(
                new PlanFragmentId(0),
                planNode,
                ImmutableSet.copyOf(planNode.getOutputVariables()),
                SOURCE_DISTRIBUTION,
                ImmutableList.of(planNode.getId()),
                new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), planNode.getOutputVariables()),
                StageExecutionDescriptor.ungroupedExecution(),
                false,
                StatsAndCosts.empty(),
                Optional.empty());

        StageId stageId = new StageId(new QueryId("query"), 0);

        // set MAX_FAILED_TASK_PERCENTAGE to 1.0 so that it is guaranteed it would not exceed the failed task ratio.
        Session session = testSessionBuilder()
                .setCatalog("tpch")
                .setSchema(TINY_SCHEMA_NAME)
                .setSystemProperty(MAX_FAILED_TASK_PERCENTAGE, String.valueOf(maxFailedTaskPercentage))
                .build();

        SqlStageExecution stage = createSqlStageExecution(
                new StageExecutionId(stageId, 0),
                plan,
                new MockRemoteTaskFactory(executor, scheduledExecutor),
                session,
                true,
                nodeTaskMap,
                executor,
                new NoOpFailureDetector(),
                new SplitSchedulerStats(),
                new TableWriteInfo(Optional.empty(), Optional.empty(), Optional.empty()), false);
        stage.setOutputBuffers(createInitialEmptyOutputBuffers(ARBITRARY));

        // add listener that fetches stage info when the final status is available
        SettableFuture<StageExecutionInfo> finalStageInfo = SettableFuture.create();
        stage.addFinalStageInfoListener(finalStageInfo::set);

        stage.beginScheduling();
        stage.transitionToSchedulingSplits();

        ArrayList<RemoteTask> allTasks = new ArrayList<RemoteTask>();
        for (int i = 0; i < 2; i++) {
            InternalNode node = new InternalNode(
                    "source" + i,
                    URI.create("http://10.0.0." + i + ":" + i),
                    NodeVersion.UNKNOWN,
                    false);
            Optional<RemoteTask> ot = stage.scheduleTask(node, i);

            if (ot.isPresent()) {
                RemoteTask t = ot.get();
                allTasks.add(t);
            }
        }

        stage.transitionToFinishedTaskScheduling();
        stage.transitionToSchedulingSplits();

        for (int taskIdx = 0; taskIdx < allTasks.size(); taskIdx += 1) {
            RemoteTask t = allTasks.get(taskIdx);
            ImmutableMultimap.Builder<PlanNodeId, Split> splits = ImmutableMultimap.builder();

            for (int j = 1; j <= 2; j += 1) {
                int connectorSplitId = taskIdx * 100 + j;
                splits.put(planNode.getId(), new Split(new ConnectorId("test"), new ConnectorTransactionHandle() {}, new MockSplitSource.MockConnectorSplit(connectorSplitId)));
            }

            t.addSplits(splits.build());
        }

        stage.registerStageTaskRecoveryCallback((taskId, executionFailureInfos) -> {
            throw new Exception("This code should not be reachable");
        }, ImmutableSet.of(HOST_SHUTTING_DOWN.toErrorCode()));

        stage.schedulingComplete();

        MockRemoteTaskFactory.MockRemoteTask firstTask = (MockRemoteTaskFactory.MockRemoteTask) allTasks.get(0);
        MockRemoteTaskFactory.MockRemoteTask secondTask = (MockRemoteTaskFactory.MockRemoteTask) allTasks.get(1);

        stage.updateTaskStatus(firstTask.getTaskId(), firstTask.getTaskStatus());
        stage.updateTaskStatus(secondTask.getTaskId(), secondTask.getTaskStatus());

        if (planNode instanceof TableScanNode) {
            assertSame(stage.getState(), StageExecutionState.DRAINING);
            assertFalse(stage.getStageExecutionInfo().getTasks().isEmpty());
            assertFalse(stage.noMoreRetry());

            // The stage needs to wait for all the tasks to become idling to be able to be marked as noMoreRetry
            assertFalse(stage.noMoreRetry());

            firstTask.markTaskIdling();

            assertFalse(stage.noMoreRetry());

            secondTask.markTaskIdling();

            assertTrue(stage.noMoreRetry());

            stage.updateTaskStatus(secondTask.getTaskId(), secondTask.getTaskStatus());
        }

        assertSame(stage.getState(), StageExecutionState.FINISHED);

        for (PlanNodeId tableScanPlanNodeId : plan.getTableScanSchedulingOrder()) {
            for (RemoteTask task : allTasks) {
                assertTrue(((MockRemoteTaskFactory.MockRemoteTask) task).noMoreSplitsReceived(tableScanPlanNodeId));
            }
        }
    }

    @Test(timeOut = 3 * 60 * 1000)
    public void testNoMoreRetryOnOneFailedTaskOutOfTwoForLeafStage()
            throws Exception
    {
        // run test a few times to catch any race conditions
        // this is not done with TestNG invocation count so there can be a global time limit on the test
        for (int iteration = 0; iteration < 10; iteration++) {
            testNoMoreRetryOnOneFailedTaskOutOfTwoForLeafStageInternal();
        }
    }

    @SuppressWarnings("checkstyle:EmptyBlock")
    private void testNoMoreRetryOnOneFailedTaskOutOfTwoForLeafStageInternal()
            throws Exception
    {
        int retryBatchSize = 100;
        double maxFailedTaskPercentage = 1.0;

        NodeTaskMap nodeTaskMap = new NodeTaskMap(new FinalizerService());

        // Using a TableScanNode to make the SqlStageExecution a leaf stage.
        PlanNode planNode = new TableScanNode(
                Optional.empty(),
                new PlanNodeId("exchange"),
                new TableHandle(new ConnectorId("test"), new ConnectorTableHandle() {}, new ConnectorTransactionHandle() {}, Optional.empty()),
                ImmutableList.of(),
                new HashMap<>(),
                TupleDomain.all(),
                TupleDomain.all());

        PlanFragment plan = new PlanFragment(
                new PlanFragmentId(0),
                planNode,
                ImmutableSet.copyOf(planNode.getOutputVariables()),
                SOURCE_DISTRIBUTION,
                ImmutableList.of(planNode.getId()),
                new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), planNode.getOutputVariables()),
                StageExecutionDescriptor.ungroupedExecution(),
                false,
                StatsAndCosts.empty(),
                Optional.empty());

        StageId stageId = new StageId(new QueryId("query"), 0);

        // set MAX_FAILED_TASK_PERCENTAGE to 1.0 so that it is guaranteed it would not exceed the failed task ratio.
        Session session = testSessionBuilder()
                .setCatalog("tpch")
                .setSchema(TINY_SCHEMA_NAME)
                .setSystemProperty(MAX_FAILED_TASK_PERCENTAGE, String.valueOf(maxFailedTaskPercentage))
                .build();

        SqlStageExecution stage = createSqlStageExecution(
                new StageExecutionId(stageId, 0),
                plan,
                new MockRemoteTaskFactory(executor, scheduledExecutor),
                session,
                true,
                nodeTaskMap,
                executor,
                new NoOpFailureDetector(),
                new SplitSchedulerStats(),
                new TableWriteInfo(Optional.empty(), Optional.empty(), Optional.empty()), false);
        stage.setOutputBuffers(createInitialEmptyOutputBuffers(ARBITRARY));

        // add listener that fetches stage info when the final status is available
        SettableFuture<StageExecutionInfo> finalStageInfo = SettableFuture.create();
        stage.addFinalStageInfoListener(finalStageInfo::set);

        stage.beginScheduling();
        stage.transitionToSchedulingSplits();

        ArrayList<RemoteTask> allTasks = new ArrayList<RemoteTask>();
        for (int i = 0; i < 2; i++) {
            InternalNode node = new InternalNode(
                    "source" + i,
                    URI.create("http://10.0.0." + i + ":" + i),
                    NodeVersion.UNKNOWN,
                    false);
            Optional<RemoteTask> ot = stage.scheduleTask(node, i);

            if (ot.isPresent()) {
                RemoteTask t = ot.get();
                allTasks.add(t);
            }
        }

        stage.transitionToFinishedTaskScheduling();
        stage.transitionToSchedulingSplits();

        for (int taskIdx = 0; taskIdx < allTasks.size(); taskIdx += 1) {
            RemoteTask t = allTasks.get(taskIdx);
            ImmutableMultimap.Builder<PlanNodeId, Split> splits = ImmutableMultimap.builder();

            for (int j = 1; j <= 2; j += 1) {
                int connectorSplitId = taskIdx * 100 + j;
                splits.put(planNode.getId(), new Split(new ConnectorId("test"), new ConnectorTransactionHandle() {}, new MockSplitSource.MockConnectorSplit(connectorSplitId)));
            }

            t.addSplits(splits.build());
        }

        CountDownLatch recoveryIsRun = new CountDownLatch(1);
        stage.registerStageTaskRecoveryCallback((taskId, executionFailureInfos) -> {
            RemoteTask failedRemoteTask = stage.getAllTasks().stream()
                    .filter(task -> task.getTaskId().equals(taskId))
                    .collect(onlyElement());

            String failedNodeId = failedRemoteTask.getNodeId();

            List<RemoteTask> activeRemoteTasks = stage.getAllTasks().stream()
                    .filter(task -> !task.getTaskId().equals(taskId))
                    .filter(task -> !task.getNodeId().equals(failedNodeId))
                    .filter(task -> task.getTaskStatus().getState() != TaskState.FAILED)
                    .collect(toList());

            PlanNodeId planNodeId = planNode.getId();
            ImmutableMultimap.Builder<PlanNodeId, Split> splits = ImmutableMultimap.builder();

            List<ScheduledSplit> unprocessedSplits = failedRemoteTask.getTaskStatus().getUnprocessedSplits();

            // assert the unprocessed splits are the splits being assigned to the task 1
            ImmutableSet<Integer> unprocessedConnectorSplitIds = unprocessedSplits.stream().map(s -> ((MockSplitSource.MockConnectorSplit) s.getSplit().getConnectorSplit()).getConnectorSplitId()).collect(toImmutableSet());
            assertEquals(unprocessedConnectorSplitIds, ImmutableSet.of(1, 2));

            Iterator<List<Split>> splitsBatchIterator = Iterables.partition(
                    Iterables.transform(unprocessedSplits.stream().filter(scheduledSplit -> scheduledSplit.getPlanNodeId() == planNodeId).collect(toList()), ScheduledSplit::getSplit),
                    retryBatchSize).iterator();

            while (splitsBatchIterator.hasNext()) {
                for (int i = 0; i < activeRemoteTasks.size() && splitsBatchIterator.hasNext(); i++) {
                    RemoteTask activeRemoteTask = activeRemoteTasks.get(i);
                    List<Split> scheduledSplit = splitsBatchIterator.next();
                    Multimap<PlanNodeId, Split> splitsToAdd = HashMultimap.create();
                    splitsToAdd.putAll(planNodeId, scheduledSplit);
                    activeRemoteTask.addSplits(splitsToAdd);
                }
            }

            failedRemoteTask.setIsRetried();
            recoveryIsRun.countDown();
        }, ImmutableSet.of(HOST_SHUTTING_DOWN.toErrorCode()));

        stage.schedulingComplete();

        MockRemoteTaskFactory.MockRemoteTask firstTask = (MockRemoteTaskFactory.MockRemoteTask) allTasks.get(0);
        MockRemoteTaskFactory.MockRemoteTask secondTask = (MockRemoteTaskFactory.MockRemoteTask) allTasks.get(1);

        stage.updateTaskStatus(firstTask.getTaskId(), firstTask.getTaskStatus());
        stage.updateTaskStatus(secondTask.getTaskId(), secondTask.getTaskStatus());

        assertSame(stage.getState(), StageExecutionState.DRAINING);

        firstTask.graceful_failed();

        assertFalse(stage.getStageExecutionInfo().getTasks().isEmpty());
        assertFalse(stage.noMoreRetry());

        if (!awaitUninterruptibly(recoveryIsRun, 10, SECONDS)) {
            throw new UncheckedTimeoutException();
        }

        // The stage needs to wait for all the tasks to become idling to be able to be marked as noMoreRetry
        assertFalse(stage.noMoreRetry());

        secondTask.markTaskIdling();

        assertTrue(stage.noMoreRetry());

        stage.updateTaskStatus(secondTask.getTaskId(), secondTask.getTaskStatus());
        assertSame(stage.getState(), StageExecutionState.FINISHED);

        for (PlanNodeId tableScanPlanNodeId : plan.getTableScanSchedulingOrder()) {
            for (RemoteTask task : allTasks) {
                assertTrue(((MockRemoteTaskFactory.MockRemoteTask) task).noMoreSplitsReceived(tableScanPlanNodeId));
            }
        }
    }

    @DataProvider(name = "failedTaskExceedingThresholdPlanNodeDataProvider")
    public Object[][] failedTaskExceedingThresholdPlanNodeDataProvider()
    {
        PlanNode tableScanNode = new TableScanNode(
                Optional.empty(),
                new PlanNodeId("exchange"),
                new TableHandle(new ConnectorId("test"), new ConnectorTableHandle() {}, new ConnectorTransactionHandle() {}, Optional.empty()),
                ImmutableList.of(),
                new HashMap<>(),
                TupleDomain.all(),
                TupleDomain.all());

        PlanNode remoteSourceNode = new RemoteSourceNode(
                Optional.empty(),
                new PlanNodeId("exchange"),
                ImmutableList.of(new PlanFragmentId(0)),
                ImmutableList.of(new VariableReferenceExpression(Optional.empty(), "column", VARCHAR)),
                false,
                Optional.empty(),
                REPARTITION);

        return new Object[][] {
                {tableScanNode, 0.3},
                {remoteSourceNode, 0.3},
                {remoteSourceNode, 1.0},
        };
    }

    @Test(timeOut = 3 * 60 * 1000, dataProvider = "failedTaskExceedingThresholdPlanNodeDataProvider")
    public void testNoMoreRetryOnFailedTasksExceedingFailurePercentage(PlanNode planNode, double maxFailedTaskPercentage)
            throws Exception
    {
        // run test a few times to catch any race conditions
        // this is not done with TestNG invocation count so there can be a global time limit on the test
        for (int iteration = 0; iteration < 1; iteration++) {
            testNoMoreRetryOnFailedTasksExceedingFailurePercentageInternal(planNode, maxFailedTaskPercentage);
        }
    }

    private void testNoMoreRetryOnFailedTasksExceedingFailurePercentageInternal(PlanNode planNode, double maxFailedTaskPercentage)
            throws Exception
    {
        NodeTaskMap nodeTaskMap = new NodeTaskMap(new FinalizerService());

        PlanFragment plan = new PlanFragment(
                new PlanFragmentId(0),
                planNode,
                ImmutableSet.copyOf(planNode.getOutputVariables()),
                SOURCE_DISTRIBUTION,
                ImmutableList.of(planNode.getId()),
                new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), planNode.getOutputVariables()),
                StageExecutionDescriptor.ungroupedExecution(),
                false,
                StatsAndCosts.empty(),
                Optional.empty());

        StageId stageId = new StageId(new QueryId("query"), 0);

        // set MAX_FAILED_TASK_PERCENTAGE to 1.0 so that it is guaranteed it would not exceed the failed task ratio.
        Session session = testSessionBuilder()
                .setCatalog("tpch")
                .setSchema(TINY_SCHEMA_NAME)
                .setSystemProperty(MAX_FAILED_TASK_PERCENTAGE, String.valueOf(maxFailedTaskPercentage))
                .build();

        SqlStageExecution stage = createSqlStageExecution(
                new StageExecutionId(stageId, 0),
                plan,
                new MockRemoteTaskFactory(executor, scheduledExecutor),
                session,
                true,
                nodeTaskMap,
                executor,
                new NoOpFailureDetector(),
                new SplitSchedulerStats(),
                new TableWriteInfo(Optional.empty(), Optional.empty(), Optional.empty()), false);
        stage.setOutputBuffers(createInitialEmptyOutputBuffers(ARBITRARY));

        // add listener that fetches stage info when the final status is available
        SettableFuture<StageExecutionInfo> finalStageInfo = SettableFuture.create();
        stage.addFinalStageInfoListener(finalStageInfo::set);

        stage.beginScheduling();
        stage.transitionToSchedulingSplits();

        ArrayList<RemoteTask> allTasks = new ArrayList<RemoteTask>();
        for (int i = 0; i < 2; i++) {
            InternalNode node = new InternalNode(
                    "source" + i,
                    URI.create("http://10.0.0." + i + ":" + i),
                    NodeVersion.UNKNOWN,
                    false);
            Optional<RemoteTask> ot = stage.scheduleTask(node, i);

            if (ot.isPresent()) {
                RemoteTask t = ot.get();
                allTasks.add(t);
            }
        }

        stage.transitionToFinishedTaskScheduling();
        stage.transitionToSchedulingSplits();

        for (int taskIdx = 0; taskIdx < allTasks.size(); taskIdx += 1) {
            RemoteTask t = allTasks.get(taskIdx);
            ImmutableMultimap.Builder<PlanNodeId, Split> splits = ImmutableMultimap.builder();

            for (int j = 1; j <= 2; j += 1) {
                int connectorSplitId = taskIdx * 100 + j;
                splits.put(planNode.getId(), new Split(new ConnectorId("test"), new ConnectorTransactionHandle() {}, new MockSplitSource.MockConnectorSplit(connectorSplitId)));
            }

            t.addSplits(splits.build());
        }

        stage.registerStageTaskRecoveryCallback((taskId, executionFailureInfos) -> {
            throw new Exception("This code should not be reachable");
        }, ImmutableSet.of(HOST_SHUTTING_DOWN.toErrorCode()));

        if (planNode instanceof TableScanNode) {
            stage.schedulingComplete();

            MockRemoteTaskFactory.MockRemoteTask firstTask = (MockRemoteTaskFactory.MockRemoteTask) allTasks.get(0);
            MockRemoteTaskFactory.MockRemoteTask secondTask = (MockRemoteTaskFactory.MockRemoteTask) allTasks.get(1);

            stage.updateTaskStatus(firstTask.getTaskId(), firstTask.getTaskStatus());
            stage.updateTaskStatus(secondTask.getTaskId(), secondTask.getTaskStatus());

            assertSame(stage.getState(), StageExecutionState.DRAINING);

            firstTask.graceful_failed();

            stage.updateTaskStatus(firstTask.getTaskId(), firstTask.getTaskStatus());
            stage.updateTaskStatus(secondTask.getTaskId(), secondTask.getTaskStatus());
        }
        else {
            MockRemoteTaskFactory.MockRemoteTask firstTask = (MockRemoteTaskFactory.MockRemoteTask) allTasks.get(0);
            MockRemoteTaskFactory.MockRemoteTask secondTask = (MockRemoteTaskFactory.MockRemoteTask) allTasks.get(1);

            firstTask.graceful_failed();

            stage.updateTaskStatus(firstTask.getTaskId(), firstTask.getTaskStatus());
            stage.updateTaskStatus(secondTask.getTaskId(), secondTask.getTaskStatus());

            // The MockRemoteTask would immediately mark the task as finished when it receives NO_MORE_SPLIT, therefore updateTaskStatus has to be put before schedulingComplete
            // This should only happen to the MockRemoteTask.
            stage.schedulingComplete();
        }

        assertFalse(stage.getStageExecutionInfo().getTasks().isEmpty());
        assertTrue(stage.noMoreRetry());
        assertSame(stage.getState(), StageExecutionState.FAILED);
    }
}
