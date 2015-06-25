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
package com.linkedin.r2.testutils.filter;

import com.linkedin.r2.filter.FilterChain;
import com.linkedin.r2.filter.NextFilter;
import com.linkedin.r2.filter.message.rest.StreamRequestFilter;
import com.linkedin.r2.filter.message.rest.StreamResponseFilter;
import com.linkedin.r2.message.rest.Messages;
import com.linkedin.r2.message.rest.Request;
import com.linkedin.r2.message.RequestContext;
import com.linkedin.r2.message.rest.Response;
import com.linkedin.r2.message.rest.RestException;
import com.linkedin.r2.message.rest.RestRequest;
import com.linkedin.r2.message.rest.RestRequestBuilder;
import com.linkedin.r2.message.rest.RestResponse;
import com.linkedin.r2.message.rest.RestResponseBuilder;
import com.linkedin.r2.message.rest.StreamException;
import com.linkedin.r2.message.rest.StreamRequest;
import com.linkedin.r2.message.rest.StreamRequestBuilder;
import com.linkedin.r2.message.rest.StreamResponse;
import com.linkedin.r2.message.rest.StreamResponseBuilder;
import com.linkedin.r2.message.streaming.DrainReader;
import com.linkedin.r2.message.streaming.EntityStreams;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Chris Pettitt
 * @version $Revision$
 */
public class FilterUtil
{
  private FilterUtil() {}

  // add a request filter and response filter to consume the entity streams
  private static FilterChain wrapFilterChain(FilterChain fc)
  {
    FilterChain drainRequestStreamFC = fc.addLast(new StreamRequestFilter()
    {
      @Override
      public void onRequest(StreamRequest request, RequestContext requestContext, Map<String, String> wireAttrs, NextFilter<StreamRequest, StreamResponse> nextFilter)
      {
        request.getEntityStream().setReader(new DrainReader());
        nextFilter.onRequest(request.builder().build(EntityStreams.emptyStream()), requestContext, wireAttrs);
      }
    });

    return drainRequestStreamFC.addFirst(new StreamResponseFilter()
    {
      @Override
      public void onResponse(StreamResponse streamResponse, RequestContext requestContext, Map<String, String> wireAttrs, NextFilter<StreamRequest, StreamResponse> nextFilter)
      {
        streamResponse.getEntityStream().setReader(new DrainReader());
        nextFilter.onResponse(streamResponse.builder().build(EntityStreams.emptyStream()), requestContext, wireAttrs);
      }

      @Override
      public void onError(Throwable throwable, RequestContext requestContext, Map<String, String> wireAttrs, NextFilter<StreamRequest, StreamResponse> nextFilter)
      {
        if (throwable instanceof StreamException)
        {
          StreamException ex = (StreamException) throwable;
          StreamResponse response = ex.getResponse();
          response.getEntityStream().setReader(new DrainReader());
          nextFilter.onError(new StreamException(response.builder().build(EntityStreams.emptyStream()), ex.getMessage(), ex.getCause()), requestContext, wireAttrs);
        }
        else
        {
          nextFilter.onError(throwable, requestContext, wireAttrs);
        }
      }
    });
  }

  public static void fireSimpleStreamRequest(FilterChain fc)
  {
    fireStreamRequest(fc, simpleStreamRequest());
  }

  public static void fireSimpleStreamResponse(FilterChain fc)
  {
    wrapFilterChain(fc).onResponse(simpleStreamResponse(), emptyRequestContext(), emptyWireAttrs());
  }

  public static void fireSimpleStreamError(FilterChain fc)
  {
    wrapFilterChain(fc).onError(simpleError(), emptyRequestContext(), emptyWireAttrs());
  }

  public static void fireStreamRequest(FilterChain fc, StreamRequest req)
  {
    wrapFilterChain(fc).onRequest(req, emptyRequestContext(), emptyWireAttrs());
  }

  public static void fireStreamRequest(FilterChain fc, StreamRequest req, Map<String, String> wireAttrs)
  {
    wrapFilterChain(fc).onRequest(req, emptyRequestContext(), wireAttrs);
  }

  public static void fireRestRequest(FilterChain fc, RestRequest req, Map<String, String> wireAttrs)
  {
    StreamRequest streamRequest = Messages.toStreamRequest(req);
    wrapFilterChain(fc).onRequest(streamRequest, emptyRequestContext(), wireAttrs);
  }

  public static void fireRestResponse(FilterChain fc, RestResponse res, Map<String, String> wireAttrs)
  {
    StreamResponse streamResponse = Messages.toStreamResponse(res);
    wrapFilterChain(fc).onResponse(streamResponse, emptyRequestContext(), wireAttrs);
  }

  public static void fireRestError(FilterChain fc, Exception ex, Map<String, String> wireAttrs)
  {
    if (ex instanceof RestException)
    {
      StreamException streamException = Messages.toStreamException((RestException)ex);
      wrapFilterChain(fc).onError(streamException, emptyRequestContext(), wireAttrs);
    }
    else
    {
      fc.onError(ex, emptyRequestContext(), wireAttrs);
    }
  }

  public static void fireUntypedRequest(FilterChain fc, Request req)
  {
    if (req instanceof StreamRequest)
    {
      fireStreamRequest(fc, (StreamRequest) req);
    }
    else
    {
      throw new IllegalArgumentException("Unexpected request type: " + req.getClass());
    }
  }

  // Fires a request, saving the local attributes, and then fires a response with the local
  // attributes.
  public static void fireStreamRequestResponse(FilterChain fc, StreamRequest req, StreamResponse res)
  {
    final RequestContext context = new RequestContext();
    wrapFilterChain(fc).onRequest(req, context, emptyWireAttrs());
    wrapFilterChain(fc).onResponse(res, context, emptyWireAttrs());
  }

  // Determines the type of the request at runtime.
  public static void fireUntypedRequestResponse(FilterChain fc, Request req, Response res)
  {
    if (req instanceof StreamRequest)
    {
      fireStreamRequestResponse(fc, (StreamRequest) req, (StreamResponse) res);
    }
    else
    {
      throw new IllegalArgumentException("Unexpected request type: " + req.getClass());
    }
  }

  public static void fireStreamRequestError(FilterChain fc, StreamRequest req, Exception ex)
  {
    final RequestContext context = new RequestContext();
    wrapFilterChain(fc).onRequest(req, context, emptyWireAttrs());
    wrapFilterChain(fc).onError(ex, context, emptyWireAttrs());
  }

  public static void fireUntypedRequestError(FilterChain fc, Request req, Exception ex)
  {
    if (req instanceof StreamRequest)
    {
      fireStreamRequestError(fc, (StreamRequest) req, ex);
    }
    else
    {
      throw new IllegalArgumentException("Unexpected request type: " + req.getClass());
    }
  }

  public static StreamRequest simpleStreamRequest()
  {
    return new StreamRequestBuilder(URI.create("simple_uri"))
            .build(EntityStreams.emptyStream());
  }

  public static StreamResponse simpleStreamResponse()
  {
    return new StreamResponseBuilder()
            .build(EntityStreams.emptyStream());
  }

  public static RestRequest simpleRestRequest()
  {
    return new RestRequestBuilder(URI.create("simple_uri"))
        .build();
  }

  public static RestResponse simpleRestResponse()
  {
    return new RestResponseBuilder()
        .build();
  }

  public static Exception simpleError()
  {
    return new Exception("test generated error");
  }

  public static Map<String, String> emptyWireAttrs()
  {
    return new HashMap<String, String>();
  }

  public static RequestContext emptyRequestContext()
  {
    return new RequestContext();
  }
}
