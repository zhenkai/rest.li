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


import java.net.URISyntaxException;
import javax.servlet.AsyncContext;
import javax.servlet.AsyncEvent;
import javax.servlet.AsyncListener;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.linkedin.r2.message.RequestContext;
import com.linkedin.r2.message.rest.RestStatus;
import com.linkedin.r2.message.rest.StreamRequest;
import com.linkedin.r2.message.rest.StreamResponse;
import com.linkedin.r2.transport.common.bridge.common.TransportCallback;
import com.linkedin.r2.transport.common.bridge.common.TransportResponse;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * Abstract Async R2 Servlet. Any servlet deriving from this class can only be used with
 * containers supporting Servlet API 3.0 or greater.
 * @author Goksel Genc
 * @version $Revision$
 */
@SuppressWarnings("serial")
public abstract class AbstractAsyncR2Servlet extends AbstractR2Servlet
{
  private static final String TRANSPORT_CALLBACK_IOEXCEPTION = "TransportCallbackIOException";

  // servlet async context timeout in ms.
  private final long _timeout;

  /**
   * Initialize the servlet, optionally using servlet-api-3.0 async API, if supported
   * by the container. The latter is checked later in init()
   */
  public AbstractAsyncR2Servlet(long timeout)
  {
    _timeout = timeout;
  }

  @Override
  public void service(final HttpServletRequest req, final HttpServletResponse resp) throws ServletException,
                                                                                                 IOException
  {
    final AsyncContext ctx = req.startAsync(req, resp);
    ctx.setTimeout(_timeout);

    final AsyncCtxSyncIOHandler ioHandler = new AsyncCtxSyncIOHandler(req.getInputStream(), resp.getOutputStream(), ctx, 1024 * 16);

    RequestContext requestContext = readRequestContext(req);

    StreamRequest streamRequest;

    try
    {
      streamRequest = readFromServletRequest(req, requestContext, ioHandler);
    }
    catch (URISyntaxException e)
    {
      writeToServletError(resp, RestStatus.BAD_REQUEST, e.toString());
      return;
    }
    catch (MessagingException e)
    {
      writeToServletError(resp, RestStatus.BAD_REQUEST, e.toString());
      return;
    }

//    ctx.addListener(new AsyncListener()
//    {
//      @Override
//      public void onTimeout(AsyncEvent event) throws IOException
//      {
////        AsyncContext ctx = event.getAsyncContext();
////        writeToServletError((HttpServletResponse) ctx.getResponse(),
////                            RestStatus.INTERNAL_SERVER_ERROR,
////                            "Server Timeout");
////        ctx.complete();
//      }
//
//      @Override
//      public void onStartAsync(AsyncEvent event) throws IOException
//      {
//        // Nothing to do here
//      }
//
//      @Override
//      public void onError(AsyncEvent event) throws IOException
//      {
////        writeToServletError((HttpServletResponse) event.getSuppliedResponse(),
////                            RestStatus.INTERNAL_SERVER_ERROR,
////                            "Server Error");
////        ctx.complete();
//      }
//
//      @Override
//      public void onComplete(AsyncEvent event) throws IOException
//      {
//        Object exception = req.getAttribute(TRANSPORT_CALLBACK_IOEXCEPTION);
//        if (exception != null)
//        {
//          throw new IOException((Throwable)exception);
//        }
//      }
//    });

    TransportCallback<StreamResponse> callback = new TransportCallback<StreamResponse>()
    {
      @Override
      public void onResponse(TransportResponse<StreamResponse> response)
      {
        try
        {
          ioHandler.startWritingResponse();
          StreamResponse streamResponse = writeResponseHeadToServletResponse(response, resp);
          streamResponse.getEntityStream().setReader(ioHandler);
          ioHandler.loop();
        }
        catch (Exception e)
        {
          throw new RuntimeException(e);
        }
      }
    };

    getDispatcher().handleRequest(streamRequest, requestContext, callback);
    ioHandler.startReadingRequest();
    ioHandler.loop();
  }

  public long getTimeout()
  {
    return _timeout;
  }
}
