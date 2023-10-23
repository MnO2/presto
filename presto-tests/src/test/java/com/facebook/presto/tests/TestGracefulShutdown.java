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
package com.facebook.presto.tests;

import com.facebook.presto.Session;
import com.facebook.presto.execution.TaskManager;
import com.facebook.presto.server.BasicQueryInfo;
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.facebook.presto.server.testing.TestingPrestoServer.TestShutdownAction;
import com.facebook.presto.spi.NodePoolType;
import com.facebook.presto.tpch.TpchPlugin;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.execution.QueryState.FINISHED;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestGracefulShutdown
{
    private static final long SHUTDOWN_TIMEOUT_MILLIS = 240_000;
    private static final long SHUTDOWN_LEAF_TIMEOUT_MILLIS = 600_000;

    private static final Session TINY_SESSION = testSessionBuilder()
            .setCatalog("tpch")
            .setSchema("tiny")
            .build();
    private static final String COORDINATOR = "coordinator";
    private static final String WORKER = "worker";

    private ListeningExecutorService executor;

    @BeforeClass
    public void setUp()
    {
        executor = MoreExecutors.listeningDecorator(newCachedThreadPool());
    }

    @AfterClass(alwaysRun = true)
    public void shutdown()
    {
        executor.shutdownNow();
    }

    @DataProvider(name = "testServerInfo")
    public static Object[][] testServerInfo()
    {
        return new Object[][] {
                {WORKER, ImmutableMap.<String, String>builder()
                        .put("node-scheduler.include-coordinator", "false")
                        .put("shutdown.grace-period", "10s")
                        .build()},
                {COORDINATOR, ImmutableMap.<String, String>builder()
                        .put("node-scheduler.include-coordinator", "false")
                        .put("shutdown.grace-period", "10s")
                        .build()},
                {COORDINATOR, ImmutableMap.<String, String>builder()
                        .put("node-scheduler.include-coordinator", "true")
                        .put("shutdown.grace-period", "10s")
                        .build()}
        };
    }

    @DataProvider(name = "testHybridServerInfo")
    public static Object[][] testHybridServerInfo()
    {
        return new Object[][] {
                {ImmutableMap.<String, String>builder()
                        .put("node-scheduler.include-coordinator", "false")
                        .put("shutdown.grace-period", "10s")
                        .build()}
        };
    }

    @Test(timeOut = SHUTDOWN_LEAF_TIMEOUT_MILLIS, dataProvider = "testServerInfo")
    public void testShutdown(String serverInstanceType, Map<String, String> properties)
            throws Exception
    {
        try (DistributedQueryRunner queryRunner = createQueryRunner(TINY_SESSION, properties)) {
            List<ListenableFuture<?>> queryFutures = new ArrayList<>();
            for (int i = 0; i < 5; i++) {
                queryFutures.add(executor.submit(() -> queryRunner.execute("SELECT COUNT(*), clerk FROM orders GROUP BY clerk")));
            }
            boolean isCoordinatorInstance = serverInstanceType.equals(COORDINATOR);
            TestingPrestoServer testServer = queryRunner.getServers()
                    .stream()
                    .filter(server -> server.isCoordinator() == isCoordinatorInstance)
                    .findFirst()
                    .get();
            if (!isCoordinatorInstance) {
                TaskManager taskManager = testServer.getTaskManager();
                while (taskManager.getAllTaskInfo().isEmpty()) {
                    MILLISECONDS.sleep(500);
                }
            }
            testServer.getGracefulShutdownHandler().requestShutdown();

            Futures.allAsList(queryFutures).get();

            List<BasicQueryInfo> queryInfos = queryRunner.getCoordinator().getQueryManager().getQueries();
            for (BasicQueryInfo info : queryInfos) {
                assertEquals(info.getState(), FINISHED);
            }

            TestShutdownAction shutdownAction = (TestShutdownAction) testServer.getShutdownAction();
            shutdownAction.waitForShutdownComplete(SHUTDOWN_TIMEOUT_MILLIS);
            assertTrue(shutdownAction.isShutdown());
        }
    }

    @Test(timeOut = SHUTDOWN_LEAF_TIMEOUT_MILLIS, dataProvider = "testHybridServerInfo", invocationCount = 10)
    public void testShutdownLeaf(Map<String, String> properties)
            throws Exception
    {
        try (DistributedQueryRunner queryRunner = createQueryRunner(TINY_SESSION, properties)) {
            List<ListenableFuture<?>> queryFutures = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                queryFutures.add(executor.submit(() -> queryRunner.execute("SELECT COUNT(*), clerk FROM orders GROUP BY clerk")));
            }

            TestingPrestoServer worker = queryRunner.getServers()
                    .stream()
                    .filter(server -> !server.isCoordinator())
                    .filter(server -> server.getNodePoolType() == NodePoolType.LEAF)
                    .findFirst()
                    .get();

            TaskManager taskManager = worker.getTaskManager();

            // wait until tasks show up on the worker
            while (taskManager.getAllTaskInfo().isEmpty()) {
                MILLISECONDS.sleep(100);
            }
            while (getQueuedDrivers(taskManager) == 0 || getRunningDrivers(taskManager) == 0) {
                MILLISECONDS.sleep(100);
            }
            worker.getGracefulShutdownHandler().requestShutdown();
            while (!worker.getGracefulShutdownHandler().isGracefulShutdownCompleted()) {
                // System.out.println("Waiting for graceful shutdown to be completed");
                //log.info("Waiting for graceful shutdown to be completed, queued driver=%s, running driver=%s, blocked drivers=%s", getQueuedDrivers(taskManager), getRunningDrivers(taskManager), getBlockedDrivers(taskManager));
                MILLISECONDS.sleep(100);
            }
            long pendingSplits = worker.getGracefulShutdownSplitTracker().getPendingSplits().values().stream().mapToLong(Set::size).sum();
            //log.info("queued driver=%s, running driver=%s, blocked drivers=%s", getQueuedDrivers(taskManager), getRunningDrivers(taskManager), getBlockedDrivers(taskManager));
            //assertEquals(getCompletedSplits(taskManager), getCompletedDrivers(taskManager));
            assertEquals(pendingSplits, getQueuedDrivers(taskManager));
        }
    }

    private long getQueuedDrivers(TaskManager taskManager)
    {
        return taskManager.getAllTaskInfo().stream().mapToLong(taskInfo -> taskInfo.getStats().getQueuedDrivers()).sum();
    }

    private long getBlockedDrivers(TaskManager taskManager)
    {
        return taskManager.getAllTaskInfo().stream().mapToLong(taskInfo -> taskInfo.getStats().getBlockedDrivers()).sum();
    }

    private long getCompletedDrivers(TaskManager taskManager)
    {
        return taskManager.getAllTaskInfo().stream().mapToLong(taskInfo -> taskInfo.getStats().getCompletedDrivers()).sum();
    }

    private long getRunningDrivers(TaskManager taskManager)
    {
        return taskManager.getAllTaskInfo().stream().mapToLong(taskInfo -> taskInfo.getStats().getRunningDrivers()).sum();
    }

    @Test(timeOut = SHUTDOWN_TIMEOUT_MILLIS)
    public void testCoordinatorShutdown()
            throws Exception
    {
        try (DistributedQueryRunner queryRunner = createQueryRunner(TINY_SESSION, ImmutableMap.of())) {
            TestingPrestoServer coordinator = queryRunner.getServers()
                    .stream()
                    .filter(TestingPrestoServer::isCoordinator)
                    .findFirst()
                    .get();

            coordinator.getGracefulShutdownHandler().requestShutdown();
            TestShutdownAction shutdownAction = (TestShutdownAction) coordinator.getShutdownAction();
            shutdownAction.waitForShutdownComplete(SHUTDOWN_TIMEOUT_MILLIS);
            assertTrue(shutdownAction.isShutdown());
        }
    }

    public static DistributedQueryRunner createQueryRunner(Session session, Map<String, String> properties)
            throws Exception
    {
        DistributedQueryRunner queryRunner = new DistributedQueryRunner(session, 3, properties);

        try {
            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");
            //queryRunner.createCatalog("tpch", "tpch", ImmutableMap.of("tpch.splits-per-node", "100"));
            return queryRunner;
        }
        catch (Exception e) {
            queryRunner.close();
            throw e;
        }
    }
}
