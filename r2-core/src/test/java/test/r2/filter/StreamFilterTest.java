/*
   Copyright (c) 2012 LinkedIn Corp.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

/* $Id$ */
package test.r2.filter;


import com.linkedin.r2.filter.Filter;
import com.linkedin.r2.filter.FilterChain;
import com.linkedin.r2.filter.FilterChains;
import com.linkedin.r2.testutils.filter.BaseFilterTest;
import com.linkedin.r2.testutils.filter.FilterUtil;
import com.linkedin.r2.testutils.filter.StreamCountFilter;

import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * @author Chris Pettitt
 * @version $Revision$
 */

public class StreamFilterTest extends BaseFilterTest
{
    private Filter _filter;
    private StreamCountFilter _beforeFilter;
    private StreamCountFilter _afterFilter;
    private FilterChain _fc;

    @Override
    @BeforeMethod
    public void setUp() throws Exception
    {
        _filter = getFilter();
        _beforeFilter = new StreamCountFilter();
        _afterFilter = new StreamCountFilter();
        _fc = FilterChains.create(_beforeFilter, _filter, _afterFilter);
    }

    @Override
    protected Filter getFilter()
    {
        return _filter;
    }

    @Test
    public void testStreamRequestCallsNextFilter()
    {
        FilterUtil.fireSimpleStreamRequest(_fc);

        Assert.assertEquals(1, _afterFilter.getStreamReqCount());
    }

    @Test
    public void testStreamResponseCallsNextFilter()
    {
        FilterUtil.fireSimpleStreamResponse(_fc);

        Assert.assertEquals(1, _beforeFilter.getStreamResCount());
    }

    @Test
    public void testRestErrorCallsNextFilter()
    {
        FilterUtil.fireSimpleStreamError(_fc);

        Assert.assertEquals(1, _beforeFilter.getStreamErrCount());
    }
}
