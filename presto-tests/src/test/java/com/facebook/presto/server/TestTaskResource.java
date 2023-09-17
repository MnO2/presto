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
package com.facebook.presto.server;

import com.facebook.airlift.http.client.HttpClient;
import com.facebook.airlift.http.client.HttpUriBuilder;
import com.facebook.airlift.http.client.Request;
import com.facebook.airlift.http.client.StringResponseHandler;
import com.facebook.airlift.http.client.jetty.JettyHttpClient;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.json.JsonModule;
import com.facebook.presto.Session;
import com.facebook.presto.execution.QueryIdGenerator;
import com.facebook.presto.execution.ScheduledSplit;
import com.facebook.presto.execution.StageInfo;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.TaskSource;
import com.facebook.presto.execution.TaskStatus;
import com.facebook.presto.execution.scheduler.TableWriteInfo;
import com.facebook.presto.metadata.HandleJsonModule;
import com.facebook.presto.metadata.HandleResolver;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.metadata.TransactionHandleJacksonModule;
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.TestingSession;
import com.facebook.presto.testing.TestingSplit;
import com.facebook.presto.testing.TestingTransactionHandle;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tests.ResultWithQueryId;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static com.facebook.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static com.facebook.airlift.http.client.Request.Builder.prepareGet;
import static com.facebook.airlift.http.client.Request.Builder.preparePost;
import static com.facebook.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static com.facebook.airlift.http.client.StringResponseHandler.createStringResponseHandler;
import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.airlift.testing.Closeables.closeQuietly;
import static com.facebook.presto.execution.TaskTestUtils.createPlanFragment;
import static com.facebook.presto.execution.buffer.OutputBuffers.BufferType.PARTITIONED;
import static com.facebook.presto.execution.buffer.OutputBuffers.createInitialEmptyOutputBuffers;
import static com.facebook.presto.server.RequestHelpers.setContentTypeHeaders;
import static com.facebook.presto.tests.tpch.TpchQueryRunner.createQueryRunner;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class TestTaskResource
{
    private QueryIdGenerator queryIdGenerator = new QueryIdGenerator();
    private TestingPrestoServer server;
    private DistributedQueryRunner queryRunner;
    private HttpClient client;
    private ObjectMapper objectMapper;
    @BeforeMethod
    public void startUp()
    {
        Injector injector = Guice.createInjector(new JsonModule(), new HandleJsonModule());
        HandleResolver handleResolver = injector.getInstance(HandleResolver.class);
        injector.injectMembers(new TransactionHandleJacksonModule(handleResolver));
        objectMapper = injector.getInstance(ObjectMapper.class);
    }

    @BeforeClass
    public void setup()
            throws Exception
    {
        this.client = new JettyHttpClient();
        this.queryRunner = createQueryRunner(ImmutableMap.of("query.client.timeout", "10s"));

        TestingPrestoServer worker = null;
        for (TestingPrestoServer server : queryRunner.getServers()) {
            if (!server.isCoordinator() && !server.isResourceManager()) {
                worker = server;
            }
        }

        this.server = worker;
    }

    @AfterClass(alwaysRun = true)
    public void teardown()
    {
        closeQuietly(server);
        closeQuietly(client);
        server = null;
        client = null;
    }

    @Test
    public void testGetTaskStatusAfterShutdownRequested()
    {
        String sql = "SELECT * FROM tpch.sf1.customer WHERE tpch.sf1.customer.nationkey = 1";
        ResultWithQueryId<MaterializedResult> result = queryRunner.executeWithQueryId(queryRunner.getDefaultSession(), sql);
        QueryId queryId = result.getQueryId();
        Optional<StageInfo> stageInfo = server.getQueryManager().getFullQueryInfo(queryId).getOutputStage();
        server.getGracefulShutdownHandler().requestShutdown();

        if (stageInfo.isPresent()) {
            Stream<TaskInfo> latestTaskInfo = stageInfo.get().getAllStages().stream()
                    .flatMap(stage -> stage.getLatestAttemptExecutionInfo().getTasks().stream());
            Iterable<TaskInfo> iterableTaskInfo = latestTaskInfo::iterator;

            for (TaskInfo taskInfo : iterableTaskInfo) {
                URI taskURI = taskStatusUri(taskInfo.getTaskId().toString());
                Request taskStatusRequest = prepareGet().setUri(taskURI).build();
                TaskStatus taskStatus = client.execute(taskStatusRequest, createJsonResponseHandler(jsonCodec(TaskStatus.class)));
                System.out.println(taskStatus.getUnprocessedSplits());
            }
        }
        else {
            fail("StageInfo not present");
        }
    }

    public URI taskStatusUri(String taskId)
    {
        String taskUri = "v1/task/" + taskId + "/status";
        return HttpUriBuilder.uriBuilderFrom(server.getBaseUrl()).replacePath(taskUri).build();
    }

    @Test
    public void testCreateOrUpdateTaskAfterShutdownRequested()
    {
        TaskId taskId = new TaskId("test", 0, 0, 0, 0);
        URI taskUpdateUri = taskUpdateUri(taskId);

        System.out.println(taskUpdateUri);
        PlanNodeId planNodeId = new PlanNodeId("planNodeId");
        List<TaskSource> sources = new ArrayList<>();
        sources.add(
                new TaskSource(
                        planNodeId,
                        ImmutableSet.of(
                                new ScheduledSplit(
                                        0,
                                        planNodeId,
                                        new Split(
                                                new ConnectorId("$remote"),
                                                TestingTransactionHandle.create(),
                                                TestingSplit.createLocalSplit()))),
                        true));

        JsonCodec<PlanFragment> planFragmentJsonCodec = JsonCodec.jsonCodec(PlanFragment.class);
        Session session = TestingSession.testSessionBuilder().build();
        TaskUpdateRequest updateRequest = new TaskUpdateRequest(
                session.toSessionRepresentation(),
                session.getIdentity().getExtraCredentials(),
                Optional.of(createPlanFragment().toBytes(planFragmentJsonCodec)),
                sources,
                createInitialEmptyOutputBuffers(PARTITIONED),
                Optional.of(new TableWriteInfo(Optional.empty(), Optional.empty(), Optional.empty())));

        System.out.println(updateRequest);
        JsonCodec<TaskUpdateRequest> taskUpdateRequestCodec = JsonCodec.jsonCodec(TaskUpdateRequest.class);
        byte[] taskUpdateRequestJson = taskUpdateRequestCodec.toBytes(updateRequest);
        System.out.println(new String(taskUpdateRequestJson, StandardCharsets.UTF_8));
        Request taskUpdateRequest = setContentTypeHeaders(false, preparePost())
                .setUri(taskUpdateUri)
                .setBodyGenerator(createStaticBodyGenerator(taskUpdateRequestJson))
                .build();

        StringResponseHandler.StringResponse stringResponse = client.execute(taskUpdateRequest, createStringResponseHandler());
        assertEquals(stringResponse.getStatusCode(), 400);
        System.out.println(stringResponse.getBody());
    }

    public URI taskUpdateUri(TaskId taskId)
    {
        String taskUri = "/v1/task/";
        return HttpUriBuilder.uriBuilderFrom(server.getBaseUrl()).appendPath(taskUri).appendPath(taskId.toString()).build();
    }
}
