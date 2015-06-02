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
package test.r2.caprep;

import com.linkedin.r2.caprep.CaptureFilter;
import com.linkedin.r2.caprep.db.TransientDb;
import com.linkedin.r2.filter.Filter;
import com.linkedin.r2.filter.message.rest.StreamFilterAdapters;
import com.linkedin.r2.message.rest.Messages;
import com.linkedin.r2.message.rest.Request;
import com.linkedin.r2.message.rest.Response;
import com.linkedin.r2.message.rest.RestRequest;
import com.linkedin.r2.message.rest.RestRequestBuilder;
import com.linkedin.r2.message.rest.RestResponse;
import com.linkedin.r2.message.rest.RestResponseBuilder;
import com.linkedin.r2.testutils.filter.FilterUtil;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author Chris Pettitt
 * @version $Revision$
 */
public abstract class AbstractCaptureFilterTest extends AbstractCapRepTest
{
  @Test
  public void testInitialCapture()
  {
    final RestRequest req = request();
    final RestResponse res = response();

    Assert.assertNull(getDb().replay(req));

    FilterUtil.fireUntypedRequestResponse(getFilterChain(), Messages.toStreamRequest(req), Messages.toStreamResponse(res));

    Assert.assertEquals(res, getDb().replay(req));
  }

  @Test
  public void testTwoDifferentRequests()
  {
    final RestRequest req1 = request();
    final RestRequest req2 = new RestRequestBuilder(req1).setEntity("This is a different request".getBytes()).build();
    final RestResponse res1 = response();
    final RestResponse res2 = new RestResponseBuilder(res1).setEntity("This is a different response".getBytes()).build();

    FilterUtil.fireUntypedRequestResponse(getFilterChain(), Messages.toStreamRequest(req1), Messages.toStreamResponse(res1));
    FilterUtil.fireUntypedRequestResponse(getFilterChain(), Messages.toStreamRequest(req2), Messages.toStreamResponse(res2));

    // Should have created two separate entries
    Assert.assertEquals(res1, getDb().replay(req1));
    Assert.assertEquals(res2, getDb().replay(req2));
  }

  @Test
  public void testSameRequestDifferentResponses()
  {
    final RestRequest req = request();
    final RestResponse res1 = response();
    final RestResponse res2 = new RestResponseBuilder().setEntity("This is a different response".getBytes()).build();

    FilterUtil.fireUntypedRequestResponse(getFilterChain(), Messages.toStreamRequest(req), Messages.toStreamResponse(res1));
    FilterUtil.fireUntypedRequestResponse(getFilterChain(), Messages.toStreamRequest(req), Messages.toStreamResponse(res2));

    // Last one wins
    Assert.assertEquals(res2, getDb().replay(req));
  }

  @Test
  public void testException()
  {
    final RestRequest req = request();
    final Exception ex = new Exception();

    FilterUtil.fireUntypedRequestError(getFilterChain(), Messages.toStreamRequest(req), ex);

    // Request / response should not be recorded
    Assert.assertNull(getDb().replay(req));
  }

  @Override
  protected Filter createFilter(TransientDb db)
  {
    return StreamFilterAdapters.adaptRestFilter(new CaptureFilter(db));
  }
}
