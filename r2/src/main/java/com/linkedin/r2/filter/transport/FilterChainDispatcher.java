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


import com.linkedin.r2.filter.FilterChain;
import com.linkedin.r2.message.RequestContext;
import com.linkedin.r2.message.rest.RestRequest;
import com.linkedin.r2.message.rest.RestResponse;
import com.linkedin.r2.message.rest.StreamRequest;
import com.linkedin.r2.message.rest.StreamResponse;
import com.linkedin.r2.message.streaming.StreamDecider;
import com.linkedin.r2.message.streaming.StreamDeciders;
import com.linkedin.r2.transport.common.bridge.common.TransportCallback;
import com.linkedin.r2.transport.common.bridge.server.StreamDispatcher;
import com.linkedin.r2.transport.common.bridge.server.TransportDispatcher;

import javax.mail.event.TransportAdapter;
import java.util.Map;

/**
 * {@link TransportDispatcher} adapter which composes a {@link TransportDispatcher} and a
 * {@link FilterChain}.
 *
 * @author Chris Pettitt
 * @version $Revision$
 */
public class FilterChainDispatcher implements StreamDispatcher
{
  private final FilterChain _filters;


  public FilterChainDispatcher(TransportDispatcher dispatcher,
                               FilterChain filters)
  {
    this(dispatcher, NO_STREAM_SUPPORT, StreamDeciders.noStream(), filters);
  }

  public FilterChainDispatcher(StreamDispatcher streamDispatcher,
                               FilterChain filters)
  {
    this(NO_REST_SUPPORT, streamDispatcher, StreamDeciders.alwaysStream(), filters);
  }

  public FilterChainDispatcher(TransportDispatcher dispatcher,
                               StreamDispatcher streamDispatcher,
                               StreamDecider streamDecider,
                               FilterChain filters)
  {
    _filters = filters
            .addFirst(new ResponseFilter())
            .addLast(new DispatcherRequestFilter(dispatcher, streamDispatcher, streamDecider));
  }

  @Override
  public void handleStreamRequest(StreamRequest req, Map<String, String> wireAttrs,
                                RequestContext requestContext,
                                TransportCallback<StreamResponse> callback)
  {
    ResponseFilter.registerCallback(callback, requestContext);
    _filters.onRequest(req, requestContext, wireAttrs);
  }

  private static final TransportDispatcher NO_REST_SUPPORT = new TransportDispatcher()
  {
    @Override
    public void handleRestRequest(RestRequest req, Map<String, String> wireAttrs, RequestContext requestContext, TransportCallback<RestResponse> callback)
    {
      throw new UnsupportedOperationException("No TransportDispatcher was provided.");
    }
  };

  private static final StreamDispatcher NO_STREAM_SUPPORT = new StreamDispatcher()
  {
    @Override
    public void handleStreamRequest(StreamRequest req, Map<String, String> wireAttrs, RequestContext requestContext, TransportCallback<StreamResponse> callback)
    {
      throw new UnsupportedOperationException("No StreamDispatcher was provided.");
    }
  };
}
