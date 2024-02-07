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
package com.facebook.presto.operator;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class ExchangeClientDebugStats
{
    public List<List<UpstreamOutputBufferStats>> upstreamOutputBufferStatsList;
    public List<Long> pollPageTimesList;
    public List<Long> scheduledRequestTimesList;
    public List<Long> fanOutCountList;
    public List<List<Long>> delayNanoSeconds;
    public long averageResponseSize;

    @JsonCreator
    public ExchangeClientDebugStats(
            @JsonProperty("upstreamOutputBufferStatsList") List<List<UpstreamOutputBufferStats>> upstreamOutputBufferStatsList,
            @JsonProperty("pollPageTimesList") List<Long> pollPageTimesList,
            @JsonProperty("scheduledRequestTimesList") List<Long> scheduledRequestTimesList,
            @JsonProperty("fanOutCountList") List<Long> fanOutCountList,
            @JsonProperty("delayNanoSeconds") List<List<Long>> delayNanoSeconds,
            @JsonProperty("averageResponseSize") long averageResponseSize)
    {
        this.upstreamOutputBufferStatsList = upstreamOutputBufferStatsList;
        this.pollPageTimesList = pollPageTimesList;
        this.scheduledRequestTimesList = scheduledRequestTimesList;
        this.fanOutCountList = fanOutCountList;
        this.delayNanoSeconds = delayNanoSeconds;
        this.averageResponseSize = averageResponseSize;
    }

    @JsonProperty
    public List<List<UpstreamOutputBufferStats>> getUpstreamOutputBufferStatsList()
    {
        return upstreamOutputBufferStatsList;
    }

    @JsonProperty
    public List<Long> getPollPageTimesList()
    {
        return pollPageTimesList;
    }

    @JsonProperty
    public List<Long> getScheduledRequestTimesList()
    {
        return scheduledRequestTimesList;
    }

    @JsonProperty
    public List<Long> getFanOutCountList()
    {
        return fanOutCountList;
    }

    @JsonProperty
    public List<List<Long>> getDelayNanoSeconds()
    {
        return delayNanoSeconds;
    }

    @JsonProperty
    public long averageResponseSize()
    {
        return averageResponseSize;
    }
}
