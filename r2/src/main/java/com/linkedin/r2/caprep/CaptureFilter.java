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
package com.linkedin.r2.caprep;


import com.linkedin.common.callback.Callback;
import com.linkedin.r2.caprep.db.DbSink;
import com.linkedin.r2.filter.NextFilter;
import com.linkedin.r2.filter.message.RequestFilter;
import com.linkedin.r2.filter.message.rest.RestResponseFilter;
import com.linkedin.r2.filter.message.rest.StreamFilter;
import com.linkedin.r2.message.rest.Messages;
import com.linkedin.r2.message.rest.Request;
import com.linkedin.r2.message.RequestContext;
import com.linkedin.r2.message.rest.Response;
import com.linkedin.r2.message.rest.RestException;
import com.linkedin.r2.message.rest.RestRequest;
import com.linkedin.r2.message.rest.RestResponse;

import java.io.IOException;
import java.util.Map;

import com.linkedin.r2.message.rest.StreamException;
import com.linkedin.r2.message.rest.StreamRequest;
import com.linkedin.r2.message.rest.StreamResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Chris Pettitt
 * @version $Revision$
 */
public class CaptureFilter implements StreamFilter
{
  private static final Logger _log = LoggerFactory.getLogger(CaptureFilter.class);

  private static final String REQ_ATTR = CaptureFilter.class.getName() + ".req";

  private final DbSink _db;

  /**
   * Construct a new instance with the specified DbSink.
   *
   * @param db DbSink to be used as the target for this filter.
   */
  public CaptureFilter(DbSink db)
  {
    _db = db;
  }

  @Override
  public void onRequest(StreamRequest req, final RequestContext requestContext, final Map<String, String> wireAttrs,
                        final NextFilter<StreamRequest, StreamResponse> nextFilter)
  {
    Messages.toRestRequest(req, new Callback<RestRequest>()
    {
      @Override
      public void onError(Throwable e)
      {
        nextFilter.onError(e, requestContext, wireAttrs);
      }

      @Override
      public void onSuccess(RestRequest result)
      {
        // Save request so that it can be associated with the response
        requestContext.putLocalAttr(REQ_ATTR, result);

        nextFilter.onRequest(Messages.toStreamRequest(result), requestContext, wireAttrs);
      }
    });

  }

  @Override
  public void onResponse(final StreamResponse res, final RequestContext requestContext,
                             final Map<String, String> wireAttrs,
                             final NextFilter<StreamRequest, StreamResponse> nextFilter)
  {
    Messages.toRestResponse(res, new Callback<RestResponse>()
    {
      @Override
      public void onError(Throwable e)
      {
        nextFilter.onError(e, requestContext, wireAttrs);
      }

      @Override
      public void onSuccess(RestResponse result)
      {
        saveResponse(result, requestContext);
        nextFilter.onResponse(Messages.toStreamResponse(result), requestContext, wireAttrs);
      }
    });
  }

  @Override
  public void onError(Throwable ex, final RequestContext requestContext,
                          final Map<String, String> wireAttrs,
                          final NextFilter<StreamRequest, StreamResponse> nextFilter)
  {
    if (ex instanceof StreamException)
    {
      Messages.toRestException((StreamException) ex, new Callback<RestException>()
      {
        @Override
        public void onError(Throwable e)
        {
          nextFilter.onError(e, requestContext, wireAttrs);
        }

        @Override
        public void onSuccess(RestException result)
        {
          saveResponse(result.getResponse(), requestContext);
        }
      });
    }

    nextFilter.onError(ex, requestContext, wireAttrs);
  }

  private void saveResponse(RestResponse res, RequestContext requestContext)
  {
    final RestRequest req = (RestRequest) requestContext.removeLocalAttr(REQ_ATTR);
    if (req != null)
    {
      _log.debug("Saving response for request: " + req.getURI());
      try
      {
        _db.record(req, res);
      }
      catch (IOException e)
      {
        _log.debug("Failed to save request", e);
      }
    }
  }
}
