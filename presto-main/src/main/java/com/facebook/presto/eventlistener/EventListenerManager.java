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
package com.facebook.presto.eventlistener;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.buffer.BufferInfo;
import com.facebook.presto.execution.buffer.OutputBufferInfo;
import com.facebook.presto.execution.executor.QueryRecoveryState;
import com.facebook.presto.server.BufferInfoWithDownstreamStatsRecord;
import com.facebook.presto.server.DownstreamStatsRecords;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.facebook.presto.spi.eventlistener.EventListener;
import com.facebook.presto.spi.eventlistener.EventListenerFactory;
import com.facebook.presto.spi.eventlistener.GracefulPreemptionEvent;
import com.facebook.presto.spi.eventlistener.QueryCompletedEvent;
import com.facebook.presto.spi.eventlistener.QueryCreatedEvent;
import com.facebook.presto.spi.eventlistener.QueryUpdatedEvent;
import com.facebook.presto.spi.eventlistener.SplitCompletedEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import org.joda.time.DateTime;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.util.PropertiesUtil.loadProperties;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class EventListenerManager
{
    private static final Logger log = Logger.get(EventListenerManager.class);
    private static final File EVENT_LISTENER_CONFIGURATION = new File("etc/event-listener.properties");
    private static final String EVENT_LISTENER_PROPERTY_NAME = "event-listener.name";

    private final Map<String, EventListenerFactory> eventListenerFactories = new ConcurrentHashMap<>();
    private final AtomicReference<Optional<EventListener>> configuredEventListener = new AtomicReference<>(Optional.empty());

    public void addEventListenerFactory(EventListenerFactory eventListenerFactory)
    {
        requireNonNull(eventListenerFactory, "eventListenerFactory is null");

        if (eventListenerFactories.putIfAbsent(eventListenerFactory.getName(), eventListenerFactory) != null) {
            throw new IllegalArgumentException(format("Event listener '%s' is already registered", eventListenerFactory.getName()));
        }
    }

    public void loadConfiguredEventListener()
            throws Exception
    {
        if (EVENT_LISTENER_CONFIGURATION.exists()) {
            Map<String, String> properties = loadProperties(EVENT_LISTENER_CONFIGURATION);
            checkArgument(
                    !isNullOrEmpty(properties.get(EVENT_LISTENER_PROPERTY_NAME)),
                    "Access control configuration %s does not contain %s",
                    EVENT_LISTENER_CONFIGURATION.getAbsoluteFile(),
                    EVENT_LISTENER_PROPERTY_NAME);
            loadConfiguredEventListener(properties);
        }
    }

    public void loadConfiguredEventListener(Map<String, String> properties)
    {
        properties = new HashMap<>(properties);
        String eventListenerName = properties.remove(EVENT_LISTENER_PROPERTY_NAME);
        checkArgument(!isNullOrEmpty(eventListenerName), "event-listener.name property must be present");
        setConfiguredEventListener(eventListenerName, properties);
    }

    @VisibleForTesting
    protected void setConfiguredEventListener(String name, Map<String, String> properties)
    {
        requireNonNull(name, "name is null");
        requireNonNull(properties, "properties is null");

        log.info("-- Loading event listener --");

        EventListenerFactory eventListenerFactory = eventListenerFactories.get(name);
        checkState(eventListenerFactory != null, "Event listener %s is not registered", name);

        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(eventListenerFactory.getClass().getClassLoader())) {
            EventListener eventListener = eventListenerFactory.create(ImmutableMap.copyOf(properties));
            this.configuredEventListener.set(Optional.of(eventListener));
        }

        log.info("-- Loaded event listener %s --", name);
    }

    public void queryCompleted(QueryCompletedEvent queryCompletedEvent)
    {
        configuredEventListener.get()
                .ifPresent(eventListener -> eventListener.queryCompleted(queryCompletedEvent));
    }

    public void queryCreated(QueryCreatedEvent queryCreatedEvent)
    {
        configuredEventListener.get()
                .ifPresent(eventListener -> eventListener.queryCreated(queryCreatedEvent));
    }

    public void queryUpdated(QueryUpdatedEvent queryUpdatedEvent)
    {
        configuredEventListener.get()
                .ifPresent(eventListener -> eventListener.queryUpdated(queryUpdatedEvent));
    }

    public void splitCompleted(SplitCompletedEvent splitCompletedEvent)
    {
        configuredEventListener.get()
                .ifPresent(eventListener -> eventListener.splitCompleted(splitCompletedEvent));
    }

    public void trackPreemptionLifeCycle(TaskId taskId, QueryRecoveryState queryRecoveryState, String downstreamStatsJson)
    {
        if (configuredEventListener.get().isPresent()) {
            configuredEventListener.get().get().trackGracefulPreemption(new GracefulPreemptionEvent(taskId.getQueryId().toString(), taskId.toString(), DateTime.now().getMillis(), queryRecoveryState.name(), "", "", downstreamStatsJson));
        }
    }

    public void trackOutputBufferInfo(TaskId taskId, QueryRecoveryState queryRecoveryState, OutputBufferInfo outputBufferInfo, List<DownstreamStatsRecords> downstreamStatsRecordsList, ObjectMapper mapper)
    {
        if (configuredEventListener.get().isPresent()) {
            List<BufferInfo> bufferInfoList = outputBufferInfo.getBuffers();
            for (BufferInfo bufferInfo : bufferInfoList) {
                try {
                    if (!bufferInfo.isFinished() && bufferInfo.getBufferedPages() > 0) {
                        Optional<DownstreamStatsRecords> downstreamStatsRecord = downstreamStatsRecordsList.stream().filter(r -> r.getBufferId() == bufferInfo.getBufferId()).findFirst();
                        if (downstreamStatsRecord.isPresent()) {
                            BufferInfoWithDownstreamStatsRecord r = new BufferInfoWithDownstreamStatsRecord(bufferInfo, downstreamStatsRecord.get());
                            configuredEventListener.get().get().trackGracefulPreemption(new GracefulPreemptionEvent(taskId.getQueryId().toString(), taskId.toString(), DateTime.now().getMillis(), queryRecoveryState.name(), bufferInfo.getBufferId().toString(), "", mapper.writeValueAsString(r)));
                        }
                    }
                }
                catch (Exception e) {
                    log.error(e);
                }
            }
        }
    }
}
