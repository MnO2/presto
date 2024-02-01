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

public class DownstreamStatsRequest
{
    public long heapMemoryUsed;
    public long bufferRetainedSizeInBytes;
    public OutputBuffers.OutputBufferId bufferId;
    public List<Long> clientGetSentTimes;
    public List<Long> clientGetResponseCalledTimes;

    public List<Long> clientDeleteSentTimes;
    public List<Long> clientDeleteResponseCalledTimes;

    @JsonCreator
    public DownstreamStatsRequest(
            @JsonProperty("bufferId") OutputBuffers.OutputBufferId bufferId,
            @JsonProperty("heapMemoryUsed") long heapMemoryUsed,
            @JsonProperty("bufferRetainedSizeInBytes") long bufferRetainedSizeInBytes,
            @JsonProperty("clientGetSentTimes") List<Long> clientGetSentTimes,
            @JsonProperty("clientGetResponseCalledTimes") List<Long> clientGetResponseCalledTimes,
            @JsonProperty("clientDeleteSentTimes") List<Long> clientDeleteSentTimes,
            @JsonProperty("clientDeleteResponseCalledTimes") List<Long> clientDeleteResponseCalledTimes)
    {
        this.heapMemoryUsed = heapMemoryUsed;
        this.bufferId = bufferId;
        this.bufferRetainedSizeInBytes = bufferRetainedSizeInBytes;
        this.clientGetSentTimes = clientGetSentTimes;
        this.clientGetResponseCalledTimes = clientGetResponseCalledTimes;
        this.clientDeleteSentTimes = clientDeleteSentTimes;
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
    public OutputBuffers.OutputBufferId getBufferId()
    {
        return bufferId;
    }

    @JsonProperty
    public List<Long> getClientGetSentTimes()
    {
        return clientGetSentTimes;
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
    public List<Long> getClientDeleteResponseCalledTimes()
    {
        return clientDeleteResponseCalledTimes;
    }
}
