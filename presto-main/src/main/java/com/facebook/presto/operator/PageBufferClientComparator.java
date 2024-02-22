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

import java.util.Comparator;

public class PageBufferClientComparator
        implements Comparator<PageBufferClient>
{
    @Override
    public int compare(PageBufferClient c1, PageBufferClient c2)
    {
        if (c1.getLeftBytes() == 0 && c2.getLeftBytes() == 0) {
            return Long.compare(c1.getFetchCount(), c2.getFetchCount());
        }
        else {
            return Long.compare(-c1.getLeftBytes(), -c2.getLeftBytes());
        }
    }
}
