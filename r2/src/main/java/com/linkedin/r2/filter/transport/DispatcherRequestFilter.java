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
package com.linkedin.r2.filter.transport;

import com.linkedin.r2.filter.NextFilter;
import com.linkedin.r2.filter.message.rest.StreamRequestFilter;
import com.linkedin.r2.message.Request;
import com.linkedin.r2.message.RequestContext;
import com.linkedin.r2.message.Response;
import com.linkedin.r2.message.rest.StreamRequest;
import com.linkedin.r2.message.rest.StreamResponse;
import com.linkedin.r2.transport.common.bridge.common.TransportCallback;
import com.linkedin.r2.transport.common.bridge.common.TransportResponse;
import com.linkedin.r2.transport.common.bridge.server.StreamDispatcher;
import com.linkedin.r2.transport.common.bridge.server.TransportDispatcher;

import java.util.HashMap;
import java.util.Map;

/**
 * Filter implementation which sends requests to a {@link TransportDispatcher} for processing.
 *
 * @author Chris Pettitt
 * @version $Revision$
 */
public class DispatcherRequestFilter implements StreamRequestFilter
{
  private final StreamDispatcher _dispatcher;

  /**
   * Construct a new instance, using the specified {@link StreamDispatcher}.
   *
   * @param dispatcher the {@link com.linkedin.r2.transport.common.bridge.server.StreamDispatcher} to be used for processing requests.C
   */
  public DispatcherRequestFilter(StreamDispatcher dispatcher)
  {
    _dispatcher = dispatcher;
  }

  @Override
  public void onRequest(StreamRequest req, RequestContext requestContext,
                            Map<String, String> wireAttrs,
                            NextFilter<StreamRequest, StreamResponse> nextFilter)
  {
    try
    {
      _dispatcher.handleStreamRequest(req, wireAttrs, requestContext, createCallback(requestContext, nextFilter));
    }
    catch (Exception e)
    {
      nextFilter.onError(e, requestContext, new HashMap<String, String>());
    }
  }

  private <REQ extends Request, RES extends Response> TransportCallback<RES> createCallback(
          final RequestContext requestContext,
          final NextFilter<REQ, RES> nextFilter)
  {
    return new TransportCallback<RES>()
    {
      @Override
      public void onResponse(TransportResponse<RES> res)
      {
        final Map<String, String> wireAttrs = res.getWireAttributes();
        if (res.hasError())
        {
          nextFilter.onError(res.getError(), requestContext, wireAttrs);
        }
        else
        {
          nextFilter.onResponse(res.getResponse(), requestContext, wireAttrs);
        }
      }
    };
  }
}
