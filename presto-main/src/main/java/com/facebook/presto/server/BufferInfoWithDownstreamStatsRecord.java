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

import com.facebook.presto.execution.buffer.BufferInfo;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class BufferInfoWithDownstreamStatsRecord
{
    private final BufferInfo bufferInfo;
    private final DownstreamStatsRecords downstreamStatsRecords;
    @JsonCreator
    public BufferInfoWithDownstreamStatsRecord(
            @JsonProperty("bufferInfo") BufferInfo bufferInfo,
            @JsonProperty("downstreamStatsRecords") DownstreamStatsRecords downstreamStatsRecords)
    {
        this.bufferInfo = bufferInfo;
        this.downstreamStatsRecords = downstreamStatsRecords;
    }

    @JsonProperty
    public BufferInfo getBufferInfo()
    {
        return bufferInfo;
    }

    @JsonProperty
    public DownstreamStatsRecords getDownstreamStatsRecords()
    {
        return downstreamStatsRecords;
    }
}