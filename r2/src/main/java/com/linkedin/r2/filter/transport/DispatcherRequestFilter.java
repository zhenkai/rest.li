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

import com.linkedin.data.ByteString;
import com.linkedin.r2.filter.NextFilter;
import com.linkedin.r2.filter.message.rest.StreamRequestFilter;
import com.linkedin.r2.message.rest.Request;
import com.linkedin.r2.message.RequestContext;
import com.linkedin.r2.message.rest.Response;
import com.linkedin.r2.message.rest.StreamRequest;
import com.linkedin.r2.message.rest.StreamResponse;
import com.linkedin.r2.message.streaming.EntityStream;
import com.linkedin.r2.message.streaming.EntityStreams;
import com.linkedin.r2.message.streaming.ReadHandle;
import com.linkedin.r2.message.streaming.Reader;
import com.linkedin.r2.message.streaming.WriteHandle;
import com.linkedin.r2.message.streaming.Writer;
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
    try
    {
      final AtomicBoolean invoked = new AtomicBoolean(false);
      TransportCallback<StreamResponse> callback = createCallback(requestContext, nextFilter, invoked);
      Connector connector = new Connector(invoked, nextFilter, requestContext, wireAttrs);
      req.getEntityStream().setReader(connector);
      EntityStream newStream = EntityStreams.newEntityStream(connector);
      _dispatcher.handleStreamRequest(req.builder().build(newStream), wireAttrs, requestContext, callback);
    }
    catch (Exception e)
    {
      nextFilter.onError(e, requestContext, new HashMap<String, String>());
    }
  }

  private <REQ extends Request, RES extends Response> TransportCallback<RES> createCallback(
          final RequestContext requestContext,
          final NextFilter<REQ, RES> nextFilter,
          final AtomicBoolean invoked)
  {
    return new TransportCallback<RES>()
    {
      @Override
      public void onResponse(TransportResponse<RES> res)
      {
        if (invoked.compareAndSet(false, true))
        {
          final Map<String, String> wireAttrs = res.getWireAttributes();
          if (res.hasError())
          {
            nextFilter.onError(res.getError(), requestContext, wireAttrs);
          } else
          {
            nextFilter.onResponse(res.getResponse(), requestContext, wireAttrs);
          }
        }
      }
    };
  }

  private static class Connector implements Reader, Writer
  {
    private WriteHandle _wh;
    private ReadHandle _rh;
    private int _outstanding;
    private final AtomicBoolean _invoked;
    private final NextFilter<StreamRequest, StreamResponse> _nextFilter;
    private final RequestContext _requestContext;
    private final Map<String, String> _wireAttrs;
    private volatile boolean _aborted;

    Connector(AtomicBoolean invoked, NextFilter<StreamRequest, StreamResponse> nextFilter,
              RequestContext requestContext, Map<String, String> wireAttrs)
    {
      _outstanding = 0;
      _invoked = invoked;
      _nextFilter = nextFilter;
      _requestContext = requestContext;
      _wireAttrs = wireAttrs;
    }

    @Override
    public void onInit(ReadHandle rh)
    {
      _rh = rh;
    }

    @Override
    public void onInit(final WriteHandle wh)
    {
      _wh = wh;
    }


    @Override
    public void onDataAvailable(ByteString data)
    {
      if (_aborted)
      {
        // drop the bytes on the floor
        _rh.request(1);
        return;
      }

      _outstanding--;
      _wh.write(data);
      int diff = _wh.remaining() - _outstanding;
      if (diff > 0)
      {
        _rh.request(diff);
        _outstanding += diff;
      }
    }

    @Override
    public void onDone()
    {
      _wh.done();
    }

    @Override
    public void onError(Throwable e)
    {
      _wh.error(e);
    }

    @Override
    public void onWritePossible()
    {
      _outstanding = _wh.remaining();
      _rh.request(_outstanding);
    }

    @Override
    public void onAbort(Throwable e)
    {
      _aborted = true;
      if (_invoked.compareAndSet(false, true))
      {
        _nextFilter.onError(e, _requestContext, _wireAttrs);
      }

      // drain the bytes in request since our entity stream to reader is aborted
      _rh.request(Integer.MAX_VALUE);
    }
  }
}
