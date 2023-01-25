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

import com.google.common.collect.ImmutableList;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

public class FaultInjectionHelper
{
    private List<Boolean> deterministicFailure;
    private final Map<String, Integer> callCounts = new ConcurrentHashMap<>();
    private Random rand;

    private FaultInjectionHelper()
    {
        this.deterministicFailure = ImmutableList.of(Boolean.TRUE, Boolean.TRUE, Boolean.TRUE, Boolean.TRUE, Boolean.TRUE, Boolean.TRUE, Boolean.TRUE);
        this.rand = new Random();
    }

    public synchronized boolean isRouteToSuccess(URI uri)
    {
        if (uri.getPath().endsWith("1.0.0/status")) {
            int dice = rand.nextInt(100);
            return (dice < 0);
        }
        else if (uri.getPath().endsWith("1.0.1/status") || uri.getPath().endsWith("1.0.2/status") || uri.getPath().endsWith("1.0.3/status")) {
            Integer callCount = callCounts.getOrDefault(uri.getPath(), 0);
            callCounts.put(uri.getPath(), callCount + 1);
            if (callCount < deterministicFailure.size()) {
                return deterministicFailure.get(callCount);
            }
            else {
                int dice = rand.nextInt(100);
                return (dice < 100);
            }
        }
        else {
            return true;
        }
    }

    private static class MyWrapper
    {
        static FaultInjectionHelper instance = new FaultInjectionHelper();
    }

    public static FaultInjectionHelper getInstance()
    {
        return MyWrapper.instance;
    }
}
