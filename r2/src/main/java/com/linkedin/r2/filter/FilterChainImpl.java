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
import com.linkedin.r2.filter.message.rest.RestRequestFilter;
import com.linkedin.r2.filter.message.rest.RestResponseFilter;
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
  private final List<MessageFilter> _streamFilters;

  public FilterChainImpl()
  {
    _streamFilters = Collections.emptyList();
  }

  private FilterChainImpl(List<MessageFilter> streamFilters)
  {
    _streamFilters = Collections.unmodifiableList(new ArrayList<MessageFilter>(streamFilters));
  }

  @Override
  public FilterChain addFirst(Filter filter)
  {
    return  new FilterChainImpl(doAddFirst(_streamFilters, adaptFilter(filter)));
  }

  @Override
  public FilterChain addLast(Filter filter)
  {
    return new FilterChainImpl(doAddLast(_streamFilters, adaptFilter(filter)));
  }

  @Override
  public void onRequest(StreamRequest req, RequestContext requestContext,
                            Map<String, String> wireAttrs)
  {
    new FilterChainIterator<StreamRequest, StreamResponse>(_streamFilters, 0)
            .onRequest(req, requestContext, wireAttrs);
  }

  @Override
  public void onResponse(StreamResponse res, RequestContext requestContext,
                             Map<String, String> wireAttrs)
  {
    new FilterChainIterator<StreamRequest, StreamResponse>(_streamFilters, _streamFilters.size())
            .onResponse(res, requestContext, wireAttrs);
  }

  @Override
  public void onError(Exception ex, RequestContext requestContext,
                          Map<String, String> wireAttrs)
  {
    new FilterChainIterator<StreamRequest, StreamResponse>(_streamFilters, _streamFilters.size())
            .onError(ex, requestContext, wireAttrs);
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

  private static MessageFilter adaptFilter(Filter filter)
  {
    final RequestFilter reqFilter;
    if (filter instanceof RestRequestFilter)
    {
      reqFilter = adaptRestRequestFilter((RestRequestFilter) filter);
    }
    else if (filter instanceof StreamRequestFilter)
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
    if (filter instanceof RestResponseFilter)
    {
      resFilter = adaptRestResponseFilter((RestResponseFilter) filter);
    }
    else if (filter instanceof StreamResponseFilter)
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

  private static RequestFilter adaptStreamRequestFilter(final StreamRequestFilter streamFilter)
  {
    return new StreamRequestFilterAdapter(streamFilter);
  }

  private static ResponseFilter adaptStreamResponseFilter(final StreamResponseFilter streamFilter)
  {
    return new StreamResponseFilterAdapter(streamFilter);
  }

  @SuppressWarnings("unchecked")
  private static NextFilter<StreamRequest, StreamResponse> adaptStreamNextFilter(NextFilter<?, ?> nextFilter)
  {
    return (NextFilter<StreamRequest, StreamResponse>)nextFilter;
  }

  private static final class StreamRequestFilterAdapter implements RequestFilter
  {
    private final StreamRequestFilter _streamFilter;

    private StreamRequestFilterAdapter(StreamRequestFilter streamFilter)
    {
      _streamFilter = streamFilter;
    }

    @Override
    public void onRequest(Request req,
                          RequestContext requestContext,
                          Map<String, String> wireAttrs,
                          NextFilter<Request, Response> nextFilter)
    {
      _streamFilter.onRequest((StreamRequest) req,
          requestContext,
          wireAttrs,
          adaptStreamNextFilter(nextFilter));
    }
  }

  private static final class StreamResponseFilterAdapter implements ResponseFilter
  {
    private final StreamResponseFilter _streamFilter;

    private StreamResponseFilterAdapter(StreamResponseFilter streamFilter)
    {
      _streamFilter = streamFilter;
    }

    @Override
    public void onResponse(Response res, RequestContext requestContext,
                           Map<String, String> wireAttrs,
                           NextFilter<Request, Response> nextFilter)
    {
      _streamFilter.onResponse((StreamResponse) res,
          requestContext,
          wireAttrs,
          adaptStreamNextFilter(nextFilter));
    }

    @Override
    public void onError(Throwable ex,
                        RequestContext requestContext,
                        Map<String, String> wireAttrs,
                        NextFilter<Request, Response> nextFilter)
    {
      _streamFilter.onError(ex,
          requestContext,
          wireAttrs,
          adaptStreamNextFilter(nextFilter));
    }
  }

  // ------------------ Rest filters -------------------------
  private static RequestFilter adaptRestRequestFilter(final RestRequestFilter restFilter)
  {
    return new RestRequestFilterAdapter(restFilter);
  }

  private static ResponseFilter adaptRestResponseFilter(final RestResponseFilter restFilter)
  {
    return new RestResponseFilterAdapter(restFilter);
  }

  private static final class RestRequestFilterAdapter implements RequestFilter
  {
    private final RestRequestFilter _restFilter;

    private RestRequestFilterAdapter(RestRequestFilter restFilter)
    {
      _restFilter = restFilter;
    }

    @Override
    public void onRequest(Request req,
                          RequestContext requestContext,
                          Map<String, String> wireAttrs,
                          NextFilter<Request, Response> nextFilter)
    {
      if (req instanceof RestRequest)
      {
        _restFilter.onRestRequest((RestRequest) req, requestContext, wireAttrs, new RestToStreamNextFilterAdapter(nextFilter));
      }
      else
      {
        nextFilter.onRequest(req, requestContext, wireAttrs);
      }
    }
  }

  private static final class RestResponseFilterAdapter implements ResponseFilter
  {
    private final RestResponseFilter _restFilter;

    private RestResponseFilterAdapter(RestResponseFilter restFilter)
    {
      _restFilter = restFilter;
    }

    @Override
    public void onResponse(Response res,
                           RequestContext requestContext,
                           Map<String, String> wireAttrs,
                           NextFilter<Request, Response> nextFilter)
    {
      if (res instanceof RestResponse)
      {
        _restFilter.onRestResponse((RestResponse) res, requestContext, wireAttrs, new RestToStreamNextFilterAdapter(nextFilter));
      }
      else
      {
        nextFilter.onResponse(res, requestContext, wireAttrs);
      }
    }

    @Override
    public void onError(Throwable ex,
                        RequestContext requestContext,
                        Map<String, String> wireAttrs,
                        NextFilter<Request, Response> nextFilter)
    {
      // TODO: cannot really tell whether rest or stream
    }
  }

  private static class RestToStreamNextFilterAdapter implements NextFilter<RestRequest, RestResponse>
  {
    private final NextFilter<StreamRequest, StreamResponse> _nextFilter;

    RestToStreamNextFilterAdapter(NextFilter<Request, Response> nextFilter)
    {
      _nextFilter = adaptStreamNextFilter(nextFilter);
    }

    @Override
    public void onRequest(RestRequest request, RequestContext requestContext, Map<String, String> wireAttrs)
    {
      _nextFilter.onRequest(request, requestContext, wireAttrs);
    }

    @Override
    public void onResponse(RestResponse response, RequestContext requestContext, Map<String, String> wireAttrs)
    {
      _nextFilter.onResponse(response, requestContext, wireAttrs);
    }

    @Override
    public void onError(Throwable ex, RequestContext requestContext, Map<String, String> wireAttrs)
    {
      _nextFilter.onError(ex, requestContext, wireAttrs);
    }
  }
}
