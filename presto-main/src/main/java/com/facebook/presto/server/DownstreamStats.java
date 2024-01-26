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

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

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

    public static class Entry
    {
        public long heapMemoryUsed;
        public long bufferRetainedSizeInBytes;
        public long serverReceivedTime;
        public long clientSentTime;

        public Entry(long memoryUsage, long bufferRetainedSizeInBytes, long serverReceivedTime, long clientSentTime)
        {
            this.heapMemoryUsed = memoryUsage;
            this.bufferRetainedSizeInBytes = bufferRetainedSizeInBytes;
            this.serverReceivedTime = serverReceivedTime;
            this.clientSentTime = clientSentTime;
        }
    }
}
