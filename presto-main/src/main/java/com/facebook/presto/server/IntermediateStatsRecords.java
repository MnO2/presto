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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class IntermediateStatsRecords
{
    private double outputBufferUtilization;
    private List<Integer> levelSizes;

    @JsonCreator
    public IntermediateStatsRecords(
            @JsonProperty("outputBufferUtilization") double outputBufferUtilization,
            @JsonProperty("levelSizes") List<Integer> levelSizes)
    {
        this.outputBufferUtilization = outputBufferUtilization;
        this.levelSizes = levelSizes;
    }

    @JsonProperty
    public double getOutputBufferUtilization()
    {
        return outputBufferUtilization;
    }

    @JsonProperty
    public List<Integer> getLevelSizes()
    {
        return levelSizes;
    }
}
