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

package com.linkedin.r2.filter;


import com.linkedin.r2.filter.message.MessageFilter;
import com.linkedin.r2.filter.message.RequestFilter;
import com.linkedin.r2.filter.message.ResponseFilter;
import com.linkedin.r2.filter.message.rest.StreamRequestFilter;
import com.linkedin.r2.filter.message.rest.StreamResponseFilter;
import com.linkedin.r2.message.Request;
import com.linkedin.r2.message.RequestContext;
import com.linkedin.r2.message.Response;
import com.linkedin.r2.message.rest.RestRequest;
import com.linkedin.r2.message.rest.RestResponse;
import com.linkedin.r2.message.rest.StreamRequest;
import com.linkedin.r2.message.rest.StreamResponse;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * @author Chris Pettitt
 */
/* package private */ class FilterChainImpl implements FilterChain
{
  private final List<MessageFilter> _restFilters;

  public FilterChainImpl()
  {
    _restFilters = Collections.emptyList();
  }

  private FilterChainImpl(List<MessageFilter> restFilters)
  {
    _restFilters = Collections.unmodifiableList(new ArrayList<MessageFilter>(restFilters));
  }

  @Override
  public FilterChain addFirst(Filter filter)
  {
    return new FilterChainImpl(addFirstRest(filter));
  }

  @Override
  public FilterChain addLast(Filter filter)
  {
    return new FilterChainImpl(addLastRest(filter));
  }

  @Override
  public void onRequest(StreamRequest req, RequestContext requestContext,
                            Map<String, String> wireAttrs)
  {
    new FilterChainIterator<StreamRequest, StreamResponse>(_restFilters, 0)
            .onRequest(req, requestContext, wireAttrs);
  }

  @Override
  public void onResponse(StreamResponse res, RequestContext requestContext,
                             Map<String, String> wireAttrs)
  {
    new FilterChainIterator<StreamRequest, StreamResponse>(_restFilters, _restFilters.size())
            .onResponse(res, requestContext, wireAttrs);
  }

  @Override
  public void onError(Exception ex, RequestContext requestContext,
                          Map<String, String> wireAttrs)
  {
    new FilterChainIterator<StreamRequest, StreamResponse>(_restFilters, _restFilters.size())
            .onError(ex, requestContext, wireAttrs);
  }

  private List<MessageFilter> addFirstRest(Filter filter)
  {
    return doAddFirst(_restFilters, adaptRestFilter(filter));
  }

  private List<MessageFilter> addLastRest(Filter filter)
  {
    return doAddLast(_restFilters, adaptRestFilter(filter));
  }

  private <T> List<T> doAddFirst(List<T> list, T obj)
  {
    final List<T> newFilters = new ArrayList<T>(list.size() + 1);
    newFilters.add(obj);
    newFilters.addAll(list);
    return newFilters;
  }

  private <T> List<T> doAddLast(List<T> list, T obj)
  {
    final List<T> newFilters = new ArrayList<T>(list.size() + 1);
    newFilters.addAll(list);
    newFilters.add(obj);
    return newFilters;
  }

  private static MessageFilter adaptRestFilter(Filter filter)
  {
    final RequestFilter reqFilter;
    if (filter instanceof StreamRequestFilter)
    {
      reqFilter = adaptStreamRequestFilter((StreamRequestFilter) filter);
    }
    else if (filter instanceof RequestFilter)
    {
      reqFilter = (RequestFilter) filter;
    }
    else
    {
      reqFilter = null;
    }

    final ResponseFilter resFilter;
    if (filter instanceof StreamResponseFilter)
    {
      resFilter = adaptStreamResponseFilter((StreamResponseFilter) filter);
    }
    else if (filter instanceof ResponseFilter)
    {
      resFilter = (ResponseFilter) filter;
    }
    else
    {
      resFilter = null;
    }

    return new ComposedFilter(reqFilter, resFilter);
  }

  private static RequestFilter adaptStreamRequestFilter(final StreamRequestFilter restFilter)
  {
    return new StreamRequestFilterAdapter(restFilter);
  }

  private static ResponseFilter adaptStreamResponseFilter(final StreamResponseFilter restFilter)
  {
    return new StreamResponseFilterAdapter(restFilter);
  }

  @SuppressWarnings("unchecked")
  private static NextFilter<StreamRequest, StreamResponse> adaptRestNextFilter(NextFilter<?, ?> nextFilter)
  {
    return (NextFilter<StreamRequest, StreamResponse>)nextFilter;
  }

  private static final class StreamRequestFilterAdapter implements RequestFilter
  {
    private final StreamRequestFilter _restFilter;

    private StreamRequestFilterAdapter(StreamRequestFilter restFilter)
    {
      _restFilter = restFilter;
    }

    @Override
    public void onRequest(Request req,
                          RequestContext requestContext,
                          Map<String, String> wireAttrs,
                          NextFilter<Request, Response> nextFilter)
    {
      _restFilter.onRequest((StreamRequest) req,
                                requestContext,
                                wireAttrs,
                                adaptRestNextFilter(nextFilter));
    }
  }

  private static final class StreamResponseFilterAdapter implements ResponseFilter
  {
    private final StreamResponseFilter _restFilter;

    private StreamResponseFilterAdapter(StreamResponseFilter restFilter)
    {
      _restFilter = restFilter;
    }

    @Override
    public void onResponse(Response res, RequestContext requestContext,
                           Map<String, String> wireAttrs,
                           NextFilter<Request, Response> nextFilter)
    {
      _restFilter.onResponse((StreamResponse) res,
                                 requestContext,
                                 wireAttrs,
                                 adaptRestNextFilter(nextFilter));
    }

    @Override
    public void onError(Throwable ex,
                        RequestContext requestContext,
                        Map<String, String> wireAttrs,
                        NextFilter<Request, Response> nextFilter)
    {
      _restFilter.onError(ex,
                              requestContext,
                              wireAttrs,
                              adaptRestNextFilter(nextFilter));
    }
  }
}
