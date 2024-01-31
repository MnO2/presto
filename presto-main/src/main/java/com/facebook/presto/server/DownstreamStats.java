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

import com.facebook.presto.execution.buffer.OutputBuffers;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

import static com.google.common.base.MoreObjects.toStringHelper;

public class DownstreamStats
{
    public OutputBuffers.OutputBufferId bufferId;
    public int maxSize = 10;
    private final Queue<Entry> entries = new ConcurrentLinkedQueue<>();

    public DownstreamStats(OutputBuffers.OutputBufferId bufferId)
    {
        this.bufferId = bufferId;
    }

    public void addEntry(Entry entry)
    {
        while (entries.size() >= maxSize) {
            entries.poll();
        }

        entries.add(entry);
    }

    public DownstreamStatsRecords toRecord()
    {
        return new DownstreamStatsRecords(bufferId, entries.stream().collect(Collectors.toList()));
    }

    public static class Entry
    {
        public long heapMemoryUsed;
        public long bufferRetainedSizeInBytes;
        public long serverReceivedTime;
        public long clientSentTime;

        @JsonCreator
        public Entry(
                @JsonProperty("memoryUsage") long memoryUsage,
                @JsonProperty("bufferRetainedSizeInBytes") long bufferRetainedSizeInBytes,
                @JsonProperty("serverReceivedTime") long serverReceivedTime,
                @JsonProperty("clientSentTime") long clientSentTime)
        {
            this.heapMemoryUsed = memoryUsage;
            this.bufferRetainedSizeInBytes = bufferRetainedSizeInBytes;
            this.serverReceivedTime = serverReceivedTime;
            this.clientSentTime = clientSentTime;
        }

        @JsonProperty
        public long getHeapMemoryUsed()
        {
            return heapMemoryUsed;
        }

        @JsonProperty
        public long getBufferRetainedSizeInBytes()
        {
            return bufferRetainedSizeInBytes;
        }

        @JsonProperty
        public long getServerReceivedTime()
        {
            return serverReceivedTime;
        }

        @JsonProperty
        public long getClientSentTime()
        {
            return clientSentTime;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("heapMemoryUsed", heapMemoryUsed)
                    .add("bufferRetainedSizeInBytes", bufferRetainedSizeInBytes)
                    .add("serverReceivedTime", serverReceivedTime)
                    .add("clientSentTime", clientSentTime)
                    .toString();
        }
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();

        entries.forEach(entry -> {
            builder.append(entry.toString());
        });

        return toStringHelper(this)
                .add("bufferId", bufferId)
                .add("entries", builder.toString())
                .toString();
    }
}
