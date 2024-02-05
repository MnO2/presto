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
package com.facebook.presto.spi.eventlistener;

import javax.annotation.concurrent.Immutable;

@Immutable
public class GracefulPreemptionEvent
{
    private final String queryId;
    private final String taskId;
    private final long eventTime;
    private final String state;
    private final String outputBufferId;
    private final String upstreamOutputBufferId;
    private final String extraInfo;
    private long outputBufferSize;

    public GracefulPreemptionEvent(String queryId, String taskId, long eventTime, String state, String outputBufferId, String upstreamOutputBufferId, String extraInfo, long outputBufferSize)
    {
        this.queryId = queryId;
        this.taskId = taskId;
        this.eventTime = eventTime;
        this.state = state;
        this.outputBufferId = outputBufferId;
        this.upstreamOutputBufferId = upstreamOutputBufferId;
        this.extraInfo = extraInfo;
        this.outputBufferSize = outputBufferSize;
    }

    public String getQueryId()
    {
        return queryId;
    }

    public String getTaskId()
    {
        return taskId;
    }

    public long getEventTime()
    {
        return eventTime;
    }

    public String getState()
    {
        return state;
    }

    public String getOutputBufferId()
    {
        return outputBufferId;
    }

    public String getUpstreamOutputBufferId()
    {
        return upstreamOutputBufferId;
    }

    public String getExtraInfo()
    {
        return extraInfo;
    }

    public long getOutputBufferSize()
    {
        return outputBufferSize;
    }
}
