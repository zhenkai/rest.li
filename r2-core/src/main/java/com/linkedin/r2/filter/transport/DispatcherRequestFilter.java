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
import com.linkedin.r2.message.rest.Request;
import com.linkedin.r2.message.RequestContext;
import com.linkedin.r2.message.rest.Response;
import com.linkedin.r2.message.rest.StreamRequest;
import com.linkedin.r2.message.rest.StreamResponse;
import com.linkedin.r2.message.streaming.BaseConnector;
import com.linkedin.r2.message.streaming.EntityStream;
import com.linkedin.r2.message.streaming.EntityStreams;
import com.linkedin.r2.transport.common.bridge.common.TransportCallback;
import com.linkedin.r2.transport.common.bridge.common.TransportResponse;
import com.linkedin.r2.transport.common.bridge.server.TransportDispatcher;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Filter implementation which sends requests to a {@link TransportDispatcher} for processing.
 *
 * @author Chris Pettitt
 * @version $Revision$
 */
public class DispatcherRequestFilter implements StreamRequestFilter
{
  private final TransportDispatcher _dispatcher;

  /**
   * Construct a new instance, using the specified {@link com.linkedin.r2.transport.common.bridge.server.TransportDispatcher}.
   *
   * @param dispatcher the {@link com.linkedin.r2.transport.common.bridge.server.TransportDispatcher} to be used for processing requests.C
   */
  public DispatcherRequestFilter(TransportDispatcher dispatcher)
  {
    _dispatcher = dispatcher;
  }

  @Override
  public void onRequest(StreamRequest req, RequestContext requestContext,
                            Map<String, String> wireAttrs,
                            NextFilter<StreamRequest, StreamResponse> nextFilter)
  {
    Connector connector = null;
    try
    {
      final AtomicBoolean responded = new AtomicBoolean(false);
      TransportCallback<StreamResponse> callback = createCallback(requestContext, nextFilter, responded);
      connector = new Connector(responded, nextFilter, requestContext, wireAttrs);
      req.getEntityStream().setReader(connector);
      EntityStream newStream = EntityStreams.newEntityStream(connector);
      _dispatcher.handleStreamRequest(req.builder().build(newStream), wireAttrs, requestContext, callback);
    }
    catch (Exception e)
    {
      nextFilter.onError(e, requestContext, new HashMap<String, String>());
      if (connector != null)
      {
        connector.cancel();
      }
    }
  }

  private <REQ extends Request, RES extends Response> TransportCallback<RES> createCallback(
          final RequestContext requestContext,
          final NextFilter<REQ, RES> nextFilter,
          final AtomicBoolean responded)
  {
    return new TransportCallback<RES>()
    {
      @Override
      public void onResponse(TransportResponse<RES> res)
      {
        if (responded.compareAndSet(false, true))
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
      }
    };
  }

  private static class Connector extends BaseConnector
  {
    private final AtomicBoolean _responded;
    private final NextFilter<StreamRequest, StreamResponse> _nextFilter;
    private final RequestContext _requestContext;
    private final Map<String, String> _wireAttrs;

    Connector(AtomicBoolean responded, NextFilter<StreamRequest, StreamResponse> nextFilter,
              RequestContext requestContext, Map<String, String> wireAttrs)
    {
      super();
      _responded = responded;
      _nextFilter = nextFilter;
      _requestContext = requestContext;
      _wireAttrs = wireAttrs;
    }

    @Override
    public void onAbort(Throwable e)
    {
      if (_responded.compareAndSet(false, true))
      {
        _nextFilter.onError(e, _requestContext, _wireAttrs);
      }

      super.onAbort(e);
    }

    public void cancel()
    {
      getReadHandle().cancel();
    }
  }
}
