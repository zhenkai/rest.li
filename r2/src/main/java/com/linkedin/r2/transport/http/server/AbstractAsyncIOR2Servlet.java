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
package com.linkedin.r2.transport.http.server;


import com.linkedin.r2.filter.R2Constants;
import com.linkedin.r2.message.RequestContext;
import com.linkedin.r2.message.rest.Messages;
import com.linkedin.r2.message.rest.RestStatus;
import com.linkedin.r2.message.rest.StreamException;
import com.linkedin.r2.message.rest.StreamRequest;
import com.linkedin.r2.message.rest.StreamRequestBuilder;
import com.linkedin.r2.message.rest.StreamResponse;
import com.linkedin.r2.message.streaming.EntityStream;
import com.linkedin.r2.message.streaming.EntityStreams;
import com.linkedin.r2.transport.common.WireAttributeHelper;
import com.linkedin.r2.transport.common.bridge.common.TransportCallback;
import com.linkedin.r2.transport.common.bridge.common.TransportResponse;
import com.linkedin.r2.transport.common.bridge.common.TransportResponseImpl;
import com.linkedin.r2.transport.http.common.HttpConstants;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Enumeration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.servlet.AsyncContext;
import javax.servlet.AsyncEvent;
import javax.servlet.AsyncListener;
import javax.servlet.ServletException;
import javax.servlet.ServletInputStream;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @author Steven Ihde
 * @author Chris Pettitt
 * @author Fatih Emekci
 * @version $Revision$
 */
public abstract class AbstractAsyncIOR2Servlet extends AbstractServlet
{
  private static final String TRANSPORT_CALLBACK_IOEXCEPTION = "TransportCallbackIOException";
  private static final long   serialVersionUID = 0L;

  private final long _timeout;

  public AbstractAsyncIOR2Servlet(long timeout)
  {
    _timeout = timeout;
  }

  @Override
  protected void service(final HttpServletRequest req, final HttpServletResponse resp)
          throws ServletException, IOException
  {
    RequestContext requestContext = readRequestContext(req);

    final AsyncContext ctx = req.startAsync();
    ctx.setTimeout(_timeout);

    ctx.addListener(new AsyncListener()
    {
      @Override
      public void onTimeout(AsyncEvent event) throws IOException
      {
        ctx.complete();
      }

      @Override
      public void onStartAsync(AsyncEvent event) throws IOException
      {
        // Nothing to do here
      }

      @Override
      public void onError(AsyncEvent event) throws IOException
      {
        ctx.complete();
      }

      @Override
      public void onComplete(AsyncEvent event) throws IOException
      {
        Object exception = req.getAttribute(TRANSPORT_CALLBACK_IOEXCEPTION);
        if (exception != null)
        {
          throw new IOException((Throwable)exception);
        }
      }
    });


    final WrappedAsyncContext wrappedCtx = new WrappedAsyncContext(ctx, new AtomicBoolean(false));

    StreamRequest streamRequest;
    try
    {
      streamRequest = readFromServletRequest(req, wrappedCtx);
    }
    catch (URISyntaxException e)
    {
      writeToServletError(resp, RestStatus.BAD_REQUEST, e.toString(), wrappedCtx);
      return;
    }

    TransportCallback<StreamResponse> callback = new TransportCallback<StreamResponse>()
    {
      @Override
      public void onResponse(TransportResponse<StreamResponse> response)
      {
        try
        {
          writeToServletResponse(response, (HttpServletResponse) ctx.getResponse(), wrappedCtx);
        }
        catch (IOException e)
        {
          req.setAttribute(TRANSPORT_CALLBACK_IOEXCEPTION, e);
          ctx.complete();
        }
      }
    };

    getDispatcher().handleRequest(streamRequest, requestContext, callback);

  }

  private void writeToServletResponse(TransportResponse<StreamResponse> response,
                                      HttpServletResponse resp,
                                      WrappedAsyncContext ctx)
      throws IOException
  {
    StreamResponse streamResponse = writeResponseHeadersToServletResponse(response, resp);

    ServletOutputStream os = resp.getOutputStream();
    AsyncIOResponseHandler handler = new AsyncIOResponseHandler(os, ctx.getCtx(), ctx.getOtherDirectionFinished());
    EntityStream responseStream = streamResponse.getEntityStream();
    responseStream.setReader(handler);
  }

  private void writeToServletError(HttpServletResponse resp, int statusCode, String message, WrappedAsyncContext ctx) throws IOException
  {
    StreamResponse streamResponse =
        Messages.toStreamResponse(RestStatus.responseForStatus(statusCode, message));
    writeToServletResponse(TransportResponseImpl.success(streamResponse), resp, ctx);
  }

  private StreamRequest readFromServletRequest(HttpServletRequest req,
                                                 WrappedAsyncContext ctx)
      throws IOException,
      ServletException,
      URISyntaxException
  {
    StreamRequestBuilder builder = readStreamRequestHeadersFromServletRequest(req);

    ServletInputStream is = req.getInputStream();
    AsyncIORequestHandler handler = new AsyncIORequestHandler(is, ctx.getCtx(), ctx.getOtherDirectionFinished());
    is.setReadListener(handler);
    EntityStream entityStream = EntityStreams.newEntityStream(handler);
    return builder.build(entityStream);
  }


  private static class WrappedAsyncContext
  {
    private final AsyncContext _ctx;
    private final AtomicBoolean _writeStarted;
    private final AtomicBoolean _otherDirectionFinished = new AtomicBoolean(false);

    WrappedAsyncContext(AsyncContext ctx, AtomicBoolean writeStarted)
    {
      _ctx = ctx;
      _writeStarted = writeStarted;
    }

    AsyncContext getCtx()
    {
      return _ctx;
    }

    AtomicBoolean getWriteStarted()
    {
      return _writeStarted;
    }

    AtomicBoolean getOtherDirectionFinished()
    {
      return _otherDirectionFinished;
    }

  }
}
