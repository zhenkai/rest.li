package com.linkedin.r2.filter.message.rest;

import com.linkedin.common.callback.Callback;
import com.linkedin.r2.filter.Filter;
import com.linkedin.r2.filter.NextFilter;
import com.linkedin.r2.message.RequestContext;
import com.linkedin.r2.message.Messages;
import com.linkedin.r2.message.Request;
import com.linkedin.r2.message.Response;
import com.linkedin.r2.message.rest.RestException;
import com.linkedin.r2.message.rest.RestRequest;
import com.linkedin.r2.message.rest.RestResponse;
import com.linkedin.r2.message.stream.StreamException;
import com.linkedin.r2.message.stream.StreamRequest;
import com.linkedin.r2.message.stream.StreamResponse;

import java.util.Map;

/**
 * This helper class provides methods to adapt RestFilters to StreamFilters as currently FilterChain would ignore
 * RestFilters.
 *
 * However, using RestFilters would cause the request and/or response being fully buffered in memory, negating the
 * benefits brought by R2 streaming.
 *
 * Follow the below checklist to see if it's suitable to use adapted filters.
 *
 * 1. If you expect your filter to be used to multiple services, DO NOT use adapted filters.
 *
 * 2. Make sure you absolutely need the full entity of request or response. If you only need to headers, http methods, status, etc,
 * use {@link com.linkedin.r2.filter.message.RequestFilter} and/or {@link com.linkedin.r2.filter.message.ResponseFilter} instead.
 *
 * 3. Make sure your service only receives reasonably-size requests and only returns reasonably-sized responses so that
 * loading full entities into memory is not a problem.
 *
 * 4. Ideally the adapted filters should only be used for testing/debugging purposes.
 *
 * @author Zhenkai Zhu
 */
public class StreamFilterAdapters
{
  private StreamFilterAdapters() {}

  public static Filter adaptRestFilter(Filter filter)
  {
    final StreamRequestFilter requestFilter;
    if (filter instanceof RestRequestFilter)
    {
      requestFilter = new StreamRequestFilterAdapter((RestRequestFilter)filter);
    }
    else
    {
      requestFilter = null;
    }

    final StreamResponseFilter responseFilter;
    if (filter instanceof RestResponseFilter)
    {
      responseFilter = new StreamResponseFilterAdapter((RestResponseFilter)filter);
    }
    else
    {
      responseFilter = null;
    }

    return new StreamFilter()
    {
      @Override
      public void onRequest(StreamRequest req, RequestContext requestContext, Map<String, String> wireAttrs, NextFilter<StreamRequest, StreamResponse> nextFilter)
      {
        if (requestFilter != null)
        {
          requestFilter.onRequest(req, requestContext, wireAttrs, nextFilter);
        }
        else
        {
          nextFilter.onRequest(req, requestContext, wireAttrs);
        }
      }

      @Override
      public void onResponse(StreamResponse res, RequestContext requestContext, Map<String, String> wireAttrs, NextFilter<StreamRequest, StreamResponse> nextFilter)
      {
        if (responseFilter != null)
        {
          responseFilter.onResponse(res, requestContext, wireAttrs, nextFilter);
        }
        else
        {
          nextFilter.onResponse(res, requestContext, wireAttrs);
        }
      }

      @Override
      public void onError(Throwable ex, RequestContext requestContext, Map<String, String> wireAttrs, NextFilter<StreamRequest, StreamResponse> nextFilter)
      {
        if (responseFilter != null)
        {
          responseFilter.onError(ex, requestContext, wireAttrs, nextFilter);
        }
        else
        {
          nextFilter.onError(ex, requestContext, wireAttrs);
        }
      }
    };
  }

  private static class StreamRequestFilterAdapter implements StreamRequestFilter
  {
    RestRequestFilter _filter;

    public StreamRequestFilterAdapter(RestRequestFilter filter)
    {
      _filter = filter;
    }

    @Override
    public   void onRequest(StreamRequest req,
                            final RequestContext requestContext,
                            final Map<String, String> wireAttrs,
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
          _filter.onRestRequest(result, requestContext, wireAttrs,
              adaptToRest(new AdaptingNextFilter<StreamRequest, StreamResponse>(nextFilter)));
        }
      });
    }
  }

  private static class StreamResponseFilterAdapter implements StreamResponseFilter
  {
    private final RestResponseFilter _filter;

    public StreamResponseFilterAdapter(RestResponseFilter filter)
    {
      _filter = filter;
    }

    @Override
    public void onResponse(StreamResponse res,
                    final RequestContext requestContext,
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
          _filter.onRestResponse(result, requestContext, wireAttrs,
              adaptToRest(new AdaptingNextFilter<StreamRequest, StreamResponse>(nextFilter)));
        }
      });
    }

    @Override
    public void onError(Throwable ex,
                 final RequestContext requestContext,
                 final Map<String, String> wireAttrs,
                 final NextFilter<StreamRequest, StreamResponse> nextFilter)
    {
      if (ex instanceof StreamException)
      {
        Messages.toRestException((StreamException)ex, new Callback<RestException>()
        {
          @Override
          public void onError(Throwable e)
          {
            nextFilter.onError(e, requestContext, wireAttrs);
          }

          @Override
          public void onSuccess(RestException result)
          {
            _filter.onRestError(result, requestContext, wireAttrs,
                adaptToRest(new AdaptingNextFilter<StreamRequest, StreamResponse>(nextFilter)));
          }
        });
      }
      else
      {
        _filter.onRestError(ex, requestContext, wireAttrs,
            adaptToRest(new AdaptingNextFilter<StreamRequest, StreamResponse>(nextFilter)));
      }
    }
  }

  private static class AdaptingNextFilter<REQ extends Request, RES extends Response> implements NextFilter<REQ, RES>
  {
    private final NextFilter<REQ, RES> _nextFilter;

    AdaptingNextFilter(NextFilter<REQ, RES> nextFilter)
    {
      _nextFilter = nextFilter;
    }
    public void onRequest(REQ req, RequestContext requestContext, Map<String, String> wireAttrs)
    {
      if (req instanceof RestRequest)
      {
        StreamRequest streamRequest = Messages.toStreamRequest((RestRequest)req);
        adaptToStream(_nextFilter).onRequest(streamRequest, requestContext, wireAttrs);
      }
      else
      {
        _nextFilter.onRequest(req, requestContext, wireAttrs);
      }
    }

    public void onResponse(RES res, RequestContext requestContext, Map<String, String> wireAttrs)
    {
      if (res instanceof RestResponse)
      {
        StreamResponse streamResponse = Messages.toStreamResponse((RestResponse)res);
        adaptToStream(_nextFilter).onResponse(streamResponse, requestContext, wireAttrs);
      }
      else
      {
        _nextFilter.onResponse(res, requestContext, wireAttrs);
      }
    }

    public void onError(Throwable ex, RequestContext requestContext, Map<String, String> wireAttrs)
    {
      if (ex instanceof RestException)
      {
        StreamException streamException = Messages.toStreamException((RestException)ex);
        adaptToStream(_nextFilter).onError(streamException, requestContext, wireAttrs);
      }
      else
      {
        _nextFilter.onError(ex, requestContext, wireAttrs);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static NextFilter<RestRequest, RestResponse> adaptToRest(NextFilter<?,?> nextFilter)
  {
    return (NextFilter<RestRequest, RestResponse>) nextFilter;
  }

  @SuppressWarnings("unchecked")
  private static NextFilter<StreamRequest, StreamResponse> adaptToStream(NextFilter<?,?> nextFilter)
  {
    return (NextFilter<StreamRequest, StreamResponse>) nextFilter;
  }
}
