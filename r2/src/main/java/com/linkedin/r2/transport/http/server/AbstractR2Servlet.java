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


import com.linkedin.data.ByteString;
import com.linkedin.r2.filter.R2Constants;
import com.linkedin.r2.message.RequestContext;
import com.linkedin.r2.message.rest.Messages;
import com.linkedin.r2.message.rest.RestResponse;
import com.linkedin.r2.message.rest.RestStatus;
import com.linkedin.r2.message.rest.StreamException;
import com.linkedin.r2.message.rest.StreamRequest;
import com.linkedin.r2.message.rest.StreamRequestBuilder;
import com.linkedin.r2.message.rest.StreamResponse;
import com.linkedin.r2.message.streaming.EntityStreams;
import com.linkedin.r2.message.streaming.Writer;
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
import javax.mail.MessagingException;
import javax.servlet.ServletException;
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
public abstract class AbstractR2Servlet extends AbstractServlet
{
  private static final long   serialVersionUID = 0L;

  @Override
  protected void service(final HttpServletRequest req, final HttpServletResponse resp)
          throws ServletException, IOException
  {
    final SyncIOHandler ioHandler = new SyncIOHandler(req.getInputStream(), resp.getOutputStream(), 2);

    RequestContext requestContext = readRequestContext(req);

    StreamRequest streamRequest;

    try
    {
      streamRequest = readFromServletRequest(req, ioHandler);
    }
    catch (URISyntaxException e)
    {
      writeToServletError(resp, RestStatus.BAD_REQUEST, e.toString());
      return;
    }


    TransportCallback<StreamResponse> callback = new TransportCallback<StreamResponse>()
    {
      @Override
      public void onResponse(TransportResponse<StreamResponse> response)
      {
        StreamResponse streamResponse = writeResponseHeadersToServletResponse(response, resp);
        streamResponse.getEntityStream().setReader(ioHandler);
      }
    };

    getDispatcher().handleRequest(streamRequest, requestContext, callback);

    ioHandler.loop();
  }

  protected void writeToServletError(HttpServletResponse resp, int statusCode, String message) throws IOException
  {
    RestResponse restResponse =
        RestStatus.responseForStatus(statusCode, message);
    writeResponseHeadersToServletResponse(TransportResponseImpl.success(Messages.toStreamResponse(restResponse)), resp);
    final ByteString entity = restResponse.getEntity();
    entity.write(resp.getOutputStream());
    resp.getOutputStream().close();
  }

  protected StreamRequest readFromServletRequest(HttpServletRequest req, Writer writer) throws IOException,
      ServletException,
      URISyntaxException
  {
    StreamRequestBuilder rb = readStreamRequestHeadersFromServletRequest(req);
    return rb.build(EntityStreams.newEntityStream(writer));
  }
}
