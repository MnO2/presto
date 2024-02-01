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

import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

import static com.google.common.base.MoreObjects.toStringHelper;

public class DownstreamStats
{
    public OutputBuffers.OutputBufferId bufferId;
    public int maxSize = 5;
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
        public List<Long> clientGetSentTimes;
        public List<Long> serverGetReceivedTimes;
        public List<Long> clientGetResponseCalledTimes;
        public List<Long> clientDeleteSentTimes;
        public List<Long> clientDeleteResponseCalledTimes;
        public List<Long> serverDeleteReceivedTimes;

        @JsonCreator
        public Entry(
                @JsonProperty("memoryUsage") long memoryUsage,
                @JsonProperty("bufferRetainedSizeInBytes") long bufferRetainedSizeInBytes,
                @JsonProperty("clientGetSentTimes") List<Long> clientGetSentTimes,
                @JsonProperty("serverGetReceivedTimes") List<Long> serverGetReceivedTimes,
                @JsonProperty("clientGetResponseCalledTimes") List<Long> clientGetResponseCalledTimes,
                @JsonProperty("clientDeleteSentTimes") List<Long> clientDeleteSentTimes,
                @JsonProperty("serverDeleteReceivedTimes") List<Long> serverDeleteReceivedTimes,
                @JsonProperty("clientDeleteResponseCalledTimes") List<Long> clientDeleteResponseCalledTimes)
        {
            this.heapMemoryUsed = memoryUsage;
            this.bufferRetainedSizeInBytes = bufferRetainedSizeInBytes;
            this.clientGetSentTimes = clientGetSentTimes;
            this.serverGetReceivedTimes = serverGetReceivedTimes;
            this.clientGetResponseCalledTimes = clientGetResponseCalledTimes;
            this.clientDeleteSentTimes = clientDeleteSentTimes;
            this.serverDeleteReceivedTimes = serverDeleteReceivedTimes;
            this.clientDeleteResponseCalledTimes = clientDeleteResponseCalledTimes;
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
        public List<Long> getClientGetSentTimes()
        {
            return clientGetSentTimes;
        }

        @JsonProperty
        public List<Long> getServerGetReceivedTimes()
        {
            return serverGetReceivedTimes;
        }

        @JsonProperty
        public List<Long> getClientGetResponseCalledTimes()
        {
            return clientGetResponseCalledTimes;
        }

        @JsonProperty
        public List<Long> getClientDeleteSentTimes()
        {
            return clientDeleteSentTimes;
        }

        @JsonProperty
        public List<Long> getServerDeleteReceivedTimes()
        {
            return serverDeleteReceivedTimes;
        }

        @JsonProperty
        public List<Long> getClientDeleteResponseCalledTimes()
        {
            return clientDeleteResponseCalledTimes;
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
