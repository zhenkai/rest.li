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


import com.linkedin.r2.filter.Filter;
import com.linkedin.r2.filter.NextFilter;
import com.linkedin.r2.filter.message.rest.RestFilter;
import com.linkedin.r2.filter.message.rest.RestRequestFilter;
import com.linkedin.r2.filter.message.rest.RestResponseFilter;
import com.linkedin.r2.filter.message.stream.StreamFilter;
import com.linkedin.r2.filter.message.stream.StreamRequestFilter;
import com.linkedin.r2.filter.message.stream.StreamResponseFilter;
import com.linkedin.r2.message.RequestContext;
import com.linkedin.r2.message.rest.RestRequest;
import com.linkedin.r2.message.rest.RestResponse;
import com.linkedin.r2.message.stream.StreamRequest;
import com.linkedin.r2.message.stream.StreamResponse;
import com.linkedin.util.ArgumentUtil;

import java.util.Map;

/**
 * Filter which delegates all calls to a specified Filter. The delegate can be reset
 * ("replaced") after construction.
 *
 * @author Chris Pettitt
 * @version $Revision$
 */
public class ReplaceableFilter implements StreamFilter, RestFilter
{
  private volatile Filter _filter;

  /**
   * Construct a new instance with the specified filter.
   *
   * @param filter Filter to be used as delegate.
   */
  public ReplaceableFilter(Filter filter)
  {
    setFilter(filter);
  }

  /**
   * Set the delegate filter.
   *
   * @param filter Filter to be used as delegate.
   */
  public void setFilter(Filter filter)
  {
    ArgumentUtil.notNull(filter, "filter");
    _filter = filter;
  }

  /**
   * Return the current delegate.
   *
   * @return Filter which is the current delegate.
   */
  public Filter getFilter()
  {
    return _filter;
  }

  @Override
  public void onRestRequest(RestRequest req,
                              RequestContext requestContext,
                              Map<String, String> wireAttrs,
                              NextFilter<RestRequest, RestResponse> nextFilter)
  {
    if (_filter instanceof RestRequestFilter)
    {
      ((RestRequestFilter) _filter).onRestRequest(req, requestContext, wireAttrs, nextFilter);
    }
    else
    {
      nextFilter.onRequest(req, requestContext, wireAttrs);
    }
  }

  @Override
  public void onRestResponse(RestResponse res,
                      RequestContext requestContext,
                      Map<String, String> wireAttrs,
                      NextFilter<RestRequest, RestResponse> nextFilter)
  {
    if (_filter instanceof RestResponseFilter)
    {
      ((RestResponseFilter) _filter).onRestResponse(res, requestContext, wireAttrs, nextFilter);
    }
    else
    {
      nextFilter.onResponse(res, requestContext, wireAttrs);
    }
  }

  @Override
  public void onRestError(Throwable ex,
                   RequestContext requestContext,
                   Map<String, String> wireAttrs,
                   NextFilter<RestRequest, RestResponse> nextFilter)
  {
    if (_filter instanceof RestResponseFilter)
    {
      ((RestResponseFilter) _filter).onRestError(ex, requestContext, wireAttrs, nextFilter);
    }
    else
    {
      nextFilter.onError(ex, requestContext, wireAttrs);
    }
  }

  @Override
  public void onStreamRequest(StreamRequest req,
                            RequestContext requestContext,
                            Map<String, String> wireAttrs,
                            NextFilter<StreamRequest, StreamResponse> nextFilter)
  {
    if (_filter instanceof StreamRequestFilter)
    {
      ((StreamRequestFilter) _filter).onStreamRequest(req, requestContext, wireAttrs, nextFilter);
    }
    else
    {
      nextFilter.onRequest(req, requestContext, wireAttrs);
    }
  }

  @Override
  public void onStreamResponse(StreamResponse res,
                             RequestContext requestContext,
                             Map<String, String> wireAttrs,
                             NextFilter<StreamRequest, StreamResponse> nextFilter)
  {
    if (_filter instanceof StreamResponseFilter)
    {
      ((StreamResponseFilter) _filter).onStreamResponse(res, requestContext, wireAttrs, nextFilter);
    }
    else
    {
      nextFilter.onResponse(res, requestContext, wireAttrs);
    }

  }

  @Override
  public void onStreamError(Throwable ex,
                          RequestContext requestContext,
                          Map<String, String> wireAttrs,
                          NextFilter<StreamRequest, StreamResponse> nextFilter)
  {
    if (_filter instanceof StreamResponseFilter)
    {
      ((StreamResponseFilter) _filter).onStreamError(ex, requestContext, wireAttrs, nextFilter);
    }
    else
    {
      nextFilter.onError(ex, requestContext, wireAttrs);
    }
  }
}
