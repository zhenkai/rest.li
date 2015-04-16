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
import com.linkedin.r2.filter.message.rest.StreamFilter;
import com.linkedin.r2.filter.message.rest.StreamRequestFilter;
import com.linkedin.r2.filter.message.rest.StreamResponseFilter;
import com.linkedin.r2.message.RequestContext;
import com.linkedin.r2.message.rest.RestRequest;
import com.linkedin.r2.message.rest.RestResponse;
import com.linkedin.r2.message.rest.StreamRequest;
import com.linkedin.r2.message.rest.StreamResponse;
import com.linkedin.util.ArgumentUtil;

import java.util.Map;

/**
 * Filter which delegates all calls to a specified Filter. The delegate can be reset
 * ("replaced") after construction.
 *
 * @author Chris Pettitt
 * @version $Revision$
 */
public class ReplaceableFilter implements StreamFilter
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
  public void onRequest(StreamRequest req,
                            RequestContext requestContext,
                            Map<String, String> wireAttrs,
                            NextFilter<StreamRequest, StreamResponse> nextFilter)
  {
    final Filter filter = _filter;
    ((StreamRequestFilter)filter).onRequest(req, requestContext, wireAttrs, nextFilter);
  }

  @Override
  public void onResponse(StreamResponse res,
                             RequestContext requestContext,
                             Map<String, String> wireAttrs,
                             NextFilter<StreamRequest, StreamResponse> nextFilter)
  {
    final Filter filter = _filter;
    ((StreamResponseFilter) filter).onResponse(res, requestContext, wireAttrs, nextFilter);

  }

  @Override
  public void onError(Throwable ex,
                          RequestContext requestContext,
                          Map<String, String> wireAttrs,
                          NextFilter<StreamRequest, StreamResponse> nextFilter)
  {
    final Filter filter = _filter;
    ((StreamResponseFilter) filter).onError(ex, requestContext, wireAttrs, nextFilter);
  }
}
