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

import com.linkedin.r2.caprep.ReplayFilter;
import com.linkedin.r2.caprep.db.TransientDb;
import com.linkedin.r2.filter.Filter;
import com.linkedin.r2.filter.FilterChain;
import com.linkedin.r2.filter.message.rest.StreamFilterAdapters;
import com.linkedin.r2.message.Messages;
import com.linkedin.r2.message.rest.RestRequest;
import com.linkedin.r2.message.rest.RestResponse;
import org.testng.Assert;
import org.testng.annotations.Test;
import com.linkedin.r2.testutils.filter.CaptureLastCallFilter;
import com.linkedin.r2.testutils.filter.FilterUtil;

/**
 * @author Chris Pettitt
 * @version $Revision$
 */
public abstract class AbstractReplayFilterTest extends AbstractCapRepTest
{
  @Test
  public void testReplayWithNoMatch()
  {
    final RestRequest req = request();
    final RestResponse res = response();
    final CaptureLastCallFilter captureFilter = new CaptureLastCallFilter();
    final FilterChain fc = getFilterChain()
            .addFirst(captureFilter);

    FilterUtil.fireUntypedRequestResponse(fc, Messages.toStreamRequest(req), Messages.toStreamResponse(res));

    Assert.assertEquals(captureFilter.getLastRes(), res);
  }

  @Test
  public void testReplayWithMatch()
  {
    final RestRequest req = request();
    final RestResponse res = response();
    final CaptureLastCallFilter captureFilter = new CaptureLastCallFilter();
    final FilterChain fc = getFilterChain().addFirst(captureFilter);

    // Record a response for the request we will fire
    getDb().record(req, res);

    // We should be able to fire just the request - the response should be replayed from the
    // capture we set up above.
    FilterUtil.fireUntypedRequest(fc, Messages.toStreamRequest(req));
    Assert.assertEquals(captureFilter.getLastRes(), res);
  }

  @Override
  protected Filter createFilter(TransientDb db)
  {
    return StreamFilterAdapters.adaptRestFilter(new ReplayFilter(db));
  }
}
