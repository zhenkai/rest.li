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
package com.linkedin.r2.transport.common;

import com.linkedin.common.callback.Callback;
import com.linkedin.common.callback.FutureCallback;
import com.linkedin.data.ByteString;
import com.linkedin.r2.message.RequestContext;
import com.linkedin.r2.message.rest.RestException;
import com.linkedin.r2.message.rest.RestRequest;
import com.linkedin.r2.message.rest.RestResponse;
import com.linkedin.r2.message.rest.RestResponseBuilder;
import com.linkedin.r2.message.rest.StreamException;
import com.linkedin.r2.message.rest.StreamRequest;
import com.linkedin.r2.message.rest.StreamResponse;
import com.linkedin.r2.message.streaming.FullEntityReader;
import com.linkedin.r2.message.streaming.Reader;

import java.net.URI;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Future;

/**
 * AbstractClient simplifies implementation of the {@link Client} interface, by providing
 * implementations of overloaded convenience methods implemented in terms of the most general
 * versions. Classes extending AbstractClient should implement:
 *
 *   void restRequest(RestRequest request,
 *                    RequestContext requestContext,
 *                    Callback<RestResponse> callback);
 *
 *   void rpcRequest(RpcRequest request,
 *                   RequestContext requestContext,
 *                   Callback<RpcResponse> callback);
 *
 *
 * @author Chris Pettitt
 * @version $Revision$
 */
public abstract class AbstractClient implements Client
{
  private static final RequestContext _EMPTY_CONTEXT = new RequestContext();

  @Override
  public Future<RestResponse> restRequest(RestRequest request)
  {
    return restRequest(request, _EMPTY_CONTEXT);
  }

  @Override
  public Future<RestResponse> restRequest(RestRequest request, RequestContext requestContext)
  {
    final FutureCallback<RestResponse> future = new FutureCallback<RestResponse>();
    restRequest(request, requestContext, future);
    return future;
  }

  @Override
  public void restRequest(RestRequest request, Callback<RestResponse> callback)
  {
    restRequest(request, _EMPTY_CONTEXT, callback);
  }

  @Override
  public void streamRequest(StreamRequest request, Callback<StreamResponse> callback)
  {
    streamRequest(request, _EMPTY_CONTEXT, callback);
  }

  @Override
  public void restRequest(RestRequest request, RequestContext requestContext, Callback<RestResponse> callback)
  {
    streamRequest(request, requestContext, adaptToStreamCallback(callback));
  }

  @Override
  public Map<String, Object> getMetadata(URI uri)
  {
    return Collections.emptyMap();
  }

  private static Callback<StreamResponse> adaptToStreamCallback(final Callback<RestResponse> callback)
  {
    return new Callback<StreamResponse>()
    {
      @Override
      public void onError(Throwable e)
      {
        if (e instanceof StreamException)
        {
          RestResponseBuilder builder = new RestResponseBuilder(((StreamException) e).getResponse());
          Callback<ByteString> exceptionCallback = getExceptionAssemblyFinishCallback(callback, builder, (StreamException) e);
          Reader reader = new FullEntityReader(exceptionCallback);
          ((StreamException) e).getResponse().getEntityStream().setReader(reader);
        }
        else
        {
          callback.onError(e);
        }
      }

      @Override
      public void onSuccess(StreamResponse result)
      {
        RestResponseBuilder builder = new RestResponseBuilder(result);
        Callback<ByteString> responseCallback = getResponseAssemblyFinishCallback(callback, builder);
        Reader reader = new FullEntityReader(responseCallback);
        result.getEntityStream().setReader(reader);
      }
    };
  }

  private static Callback<ByteString> getResponseAssemblyFinishCallback(final Callback<RestResponse> callback,
                                                                final RestResponseBuilder builder)
  {
    return new Callback<ByteString>()
    {
      @Override
      public void onError(Throwable e)
      {
        callback.onError(e);
      }

      @Override
      public void onSuccess(ByteString result)
      {
        RestResponse restResponse = builder.setEntity(result).build();
        callback.onSuccess(restResponse);
      }
    };
  }

  private static Callback<ByteString> getExceptionAssemblyFinishCallback(final Callback<RestResponse> callback,
                                                                final RestResponseBuilder builder,
                                                                final StreamException streamException)
  {
    return new Callback<ByteString>()
    {
      @Override
      public void onError(Throwable e)
      {
        // is it better to present user an Exception with original message & cause?
        callback.onError(e);
      }

      @Override
      public void onSuccess(ByteString result)
      {
        RestResponse restResponse = builder.setEntity(result).build();
        callback.onError(new RestException(restResponse, streamException.getMessage(), streamException.getCause()));
      }
    };
  }
}
