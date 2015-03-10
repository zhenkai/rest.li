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

import com.linkedin.common.callback.Callback;
import com.linkedin.data.ByteString;
import com.linkedin.r2.filter.NextFilter;
import com.linkedin.r2.filter.message.rest.StreamRequestFilter;
import com.linkedin.r2.message.Request;
import com.linkedin.r2.message.RequestContext;
import com.linkedin.r2.message.Response;
import com.linkedin.r2.message.rest.RestRequest;
import com.linkedin.r2.message.rest.RestRequestBuilder;
import com.linkedin.r2.message.rest.RestResponse;
import com.linkedin.r2.message.rest.StreamRequest;
import com.linkedin.r2.message.rest.StreamResponse;
import com.linkedin.r2.message.streaming.FullEntityReader;
import com.linkedin.r2.message.streaming.Reader;
import com.linkedin.r2.message.streaming.StreamDecider;
import com.linkedin.r2.transport.common.bridge.common.TransportCallback;
import com.linkedin.r2.transport.common.bridge.common.TransportResponse;
import com.linkedin.r2.transport.common.bridge.server.StreamDispatcher;
import com.linkedin.r2.transport.common.bridge.server.TransportCallbackAdapter;
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
  private final TransportDispatcher _dispatcher;
  private final StreamDispatcher _streamDispatcher;
  private final StreamDecider _streamDecider;

  /**
   * Construct a new instance, using the specified {@link TransportDispatcher}.
   *
   * @param dispatcher the {@link TransportDispatcher} to be used for processing requests.C
   */
  public DispatcherRequestFilter(TransportDispatcher dispatcher,
                                 StreamDispatcher streamDispatcher,
                                 StreamDecider streamDecider)
  {
    _dispatcher = dispatcher;
    _streamDispatcher = streamDispatcher;
    _streamDecider = streamDecider;
  }

  @Override
  public void onRequest(StreamRequest req, RequestContext requestContext,
                            Map<String, String> wireAttrs,
                            NextFilter<StreamRequest, StreamResponse> nextFilter)
  {
    try
    {
      StreamDecider.StreamDecision decision = _streamDecider.decide(req);

      switch (decision)
      {
        case STREAM_ENTITY:
          _streamDispatcher.handleStreamRequest(req, wireAttrs, requestContext, createCallback(requestContext, nextFilter));
          break;

        case FULL_ENTITY:
          Callback<ByteString> assemblyFinishCallback = getAssemblyFinishCallback(req, wireAttrs, requestContext, nextFilter);
          Reader reader = new FullEntityReader(assemblyFinishCallback);
          req.getEntityStream().setReader(reader);
          break;

        default:
          throw new UnsupportedOperationException("StreamDecision: " + decision + " is not supported");
      }
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

  private Callback<ByteString> getAssemblyFinishCallback(final StreamRequest req, final Map<String, String> wireAttrs,
                                                         final RequestContext requestContext,
                                                         final NextFilter<StreamRequest, StreamResponse> nextFilter)
  {
    return new Callback<ByteString>()
    {
      @Override
      public void onError(Throwable e)
      {
        throw new RuntimeException(e);
      }

      @Override
      public void onSuccess(ByteString result)
      {
        RestRequestBuilder builder = new RestRequestBuilder(req);
        builder.setEntity(result);
        _dispatcher.handleRestRequest(builder.build(),
            wireAttrs, requestContext, adaptToRestResponseCallback(createCallback(requestContext, nextFilter)));
      }
    };
  }

  private static TransportCallback<RestResponse> adaptToRestResponseCallback(final TransportCallback<StreamResponse> callback)
  {
    return new TransportCallback<RestResponse>()
    {
      @Override
      public void onResponse(final TransportResponse<RestResponse> response)
      {
        callback.onResponse(new TransportResponse<StreamResponse>()
        {
          @Override
          public StreamResponse getResponse()
          {
            return response.getResponse();
          }

          @Override
          public boolean hasError()
          {
            return response.hasError();
          }

          @Override
          public Throwable getError()
          {
            return response.getError();
          }

          @Override
          public Map<String, String> getWireAttributes()
          {
            return response.getWireAttributes();
          }
        });
      }
    };
  }

}
