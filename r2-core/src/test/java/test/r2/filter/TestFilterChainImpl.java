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


import com.linkedin.r2.filter.FilterChain;
import com.linkedin.r2.filter.FilterChains;
import com.linkedin.r2.message.RequestContext;
import com.linkedin.r2.message.Messages;
import com.linkedin.r2.message.rest.RestRequestBuilder;
import com.linkedin.r2.message.rest.RestResponseBuilder;
import com.linkedin.r2.testutils.filter.MessageCountFilter;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import com.linkedin.r2.testutils.filter.RestCountFilter;
import com.linkedin.r2.testutils.filter.StreamCountFilter;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author Chris Pettitt
 * @version $Revision$
 */
public class TestFilterChainImpl
{
  @Test
  public void testRequestFilter()
  {
    final MessageCountFilter filter = new MessageCountFilter();
    final FilterChain fc = FilterChains.create(filter);

    fireRestRequest(fc);
    assertMessageCounts(1, 0, 0, filter);
  }

  @Test
  public void testResponseFilter()
  {
    final MessageCountFilter filter = new MessageCountFilter();
    final FilterChain fc = FilterChains.create(filter);

    fireRestResponse(fc);
    assertMessageCounts(0, 1, 0, filter);
  }

  @Test
  public void testErrorFilter()
  {
    final MessageCountFilter filter = new MessageCountFilter();
    final FilterChain fc = FilterChains.create(filter);

    fireRestError(fc);
    assertMessageCounts(0, 0, 1, filter);
  }

  @Test
  public void testChainRequestFilters()
  {
    final MessageCountFilter filter1 = new MessageCountFilter();
    final MessageCountFilter filter2 = new MessageCountFilter();
    final MessageCountFilter filter3 = new MessageCountFilter();
    final FilterChain fc = FilterChains.create(filter1, filter2, filter3);

    fireRestRequest(fc);
    assertMessageCounts(1, 0, 0, filter1);
    assertMessageCounts(1, 0, 0, filter2);
    assertMessageCounts(1, 0, 0, filter3);
  }

  @Test
  public void testChainResponseFilters()
  {
    final MessageCountFilter filter1 = new MessageCountFilter();
    final MessageCountFilter filter2 = new MessageCountFilter();
    final MessageCountFilter filter3 = new MessageCountFilter();
    final FilterChain fc = FilterChains.create(filter1, filter2, filter3);

    fireRestResponse(fc);
    assertMessageCounts(0, 1, 0, filter1);
    assertMessageCounts(0, 1, 0, filter2);
    assertMessageCounts(0, 1, 0, filter3);
  }

  @Test
  public void testChainErrorFilters()
  {
    final MessageCountFilter filter1 = new MessageCountFilter();
    final MessageCountFilter filter2 = new MessageCountFilter();
    final MessageCountFilter filter3 = new MessageCountFilter();
    final FilterChain fc = FilterChains.create(filter1, filter2, filter3);

    fireRestError(fc);
    assertMessageCounts(0, 0, 1, filter1);
    assertMessageCounts(0, 0, 1, filter2);
    assertMessageCounts(0, 0, 1, filter3);
  }

  @Test
  public void testMixChainRequestFilters()
  {
    final MessageCountFilter filter1 = new MessageCountFilter();
    final StreamCountFilter filter2 = new StreamCountFilter();
    final RestCountFilter filter3 = new RestCountFilter();
    final FilterChain fc = FilterChains.create(filter1, filter2, filter3);

    fireRestRequest(fc);
    assertMessageCounts(1, 0, 0, filter1);
    assertStreamMessageCounts(1, 0, 0, filter2);
    assertRestMessageCounts(1, 0, 0, filter3);
  }

  @Test
  public void testMixChainResponseFilters()
  {
    final MessageCountFilter filter1 = new MessageCountFilter();
    final StreamCountFilter filter2 = new StreamCountFilter();
    final RestCountFilter filter3 = new RestCountFilter();
    final FilterChain fc = FilterChains.create(filter1, filter2, filter3);

    fireRestResponse(fc);
    assertMessageCounts(0, 1, 0, filter1);
    assertStreamMessageCounts(0, 1, 0, filter2);
    assertRestMessageCounts(0, 1, 0, filter3);
  }

  @Test
  public void testMixChainErrorFilters()
  {
    final MessageCountFilter filter1 = new MessageCountFilter();
    final StreamCountFilter filter2 = new StreamCountFilter();
    final RestCountFilter filter3 = new RestCountFilter();
    final FilterChain fc = FilterChains.create(filter1, filter2, filter3);

    fireRestError(fc);
    assertMessageCounts(0, 0, 1, filter1);
    assertStreamMessageCounts(0, 0, 1, filter2);
    assertRestMessageCounts(0, 0, 1, filter3);
  }

  private void fireRestRequest(FilterChain fc)
  {
    fc.onStreamRequest(Messages.toStreamRequest(new RestRequestBuilder(URI.create("src/test/resources/test")).build()),
                     createRequestContext(), createWireAttributes()
    );
  }

  private void fireRestResponse(FilterChain fc)
  {
    fc.onStreamResponse(Messages.toStreamResponse(new RestResponseBuilder().build()),
                      createRequestContext(), createWireAttributes()
    );
  }

  private void fireRestError(FilterChain fc)
  {
    fc.onStreamError(new Exception(),
                   createRequestContext(), createWireAttributes()
    );
  }

  private Map<String, String> createWireAttributes()
  {
    return new HashMap<String, String>();
  }

  private RequestContext createRequestContext()
  {
    return new RequestContext();
  }

  private void assertMessageCounts(int req, int res, int err, MessageCountFilter filter)
  {
    Assert.assertEquals(req, filter.getReqCount());
    Assert.assertEquals(res, filter.getResCount());
    Assert.assertEquals(err, filter.getErrCount());
  }

  private void assertStreamMessageCounts(int req, int res, int err, StreamCountFilter filter)
  {
    Assert.assertEquals(req, filter.getStreamReqCount());
    Assert.assertEquals(res, filter.getStreamResCount());
    Assert.assertEquals(err, filter.getStreamErrCount());
  }

  private void assertRestMessageCounts(int req, int res, int err, RestCountFilter filter)
  {
    Assert.assertEquals(req, filter.getRestReqCount());
    Assert.assertEquals(res, filter.getRestResCount());
    Assert.assertEquals(err, filter.getRestErrCount());
  }
}
