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
package com.facebook.presto.event;

import com.facebook.presto.execution.TaskId;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class TaskMonitor
{
    ConcurrentMap<TaskId, Double> utilizations = new ConcurrentHashMap<>();
    public void updateOutputBufferUtilization(TaskId taskId, double utilization)
    {
        utilizations.put(taskId, utilization);
    }

    public double getOutputBufferUtilization(TaskId taskId)
    {
        return utilizations.getOrDefault(taskId, 0.0);
    }
}
