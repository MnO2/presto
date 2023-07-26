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
package com.facebook.presto.failureDetector;

import com.facebook.airlift.discovery.client.ServiceDescriptor;
import com.facebook.presto.execution.SqlStageExecution;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.spi.HostAddress;

import java.util.Set;

public interface FailureDetector
{
    @FunctionalInterface
    interface HostShuttingDownCallback
    {
        void nodeShuttingDown(String nodeID);
    }

    Set<ServiceDescriptor> getFailed();

    State getState(HostAddress hostAddress);

    void registerHostShuttingDownCallback(HostShuttingDownCallback hostShuttingDownCallback);

    enum State
    {
        UNKNOWN,
        ALIVE,
        GONE,
        UNRESPONSIVE,
    }
}

