package com.linkedin.r2.filter.transport;

import com.linkedin.common.callback.Callback;
import com.linkedin.r2.filter.NextFilter;
import com.linkedin.r2.filter.message.rest.RestRequestFilter;
import com.linkedin.r2.filter.message.stream.StreamRequestFilter;
import com.linkedin.r2.message.RequestContext;
import com.linkedin.r2.message.QueryTunnelUtil;
import com.linkedin.r2.message.rest.RestRequest;
import com.linkedin.r2.message.rest.RestResponse;
import com.linkedin.r2.message.stream.StreamRequest;
import com.linkedin.r2.message.stream.StreamResponse;

import java.util.Map;

/**
 * @author Zhenkai Zhu
 */
public class ClientQueryTunnelFilter implements StreamRequestFilter, RestRequestFilter
{
  private final int _queryPostThreshold;

  public ClientQueryTunnelFilter(int queryPostThreshold)
  {
    _queryPostThreshold = queryPostThreshold;
  }


  @Override
  public void onRestRequest(RestRequest req,
                            RequestContext requestContext,
                            Map<String, String> wireAttrs,
                            NextFilter<RestRequest, RestResponse> nextFilter)
  {
    final RestRequest newReq;
    try
    {
      newReq = QueryTunnelUtil.encode(req, requestContext, _queryPostThreshold);
    }
    catch (Exception e)
    {
      nextFilter.onError(e, requestContext, wireAttrs);
      return;
    }
    nextFilter.onRequest(newReq, requestContext, wireAttrs);
  }

  @Override
  public void onStreamRequest(final StreamRequest req,
                     final RequestContext requestContext,
                     final Map<String, String> wireAttrs,
                     final NextFilter<StreamRequest, StreamResponse> nextFilter)
  {
    Callback<StreamRequest> callback = new Callback<StreamRequest>()
    {
      @Override
      public void onError(Throwable e)
      {
        nextFilter.onError(e, requestContext, wireAttrs);
      }

      @Override
      public void onSuccess(StreamRequest newReq)
      {
        nextFilter.onRequest(newReq, requestContext, wireAttrs);
      }
    };

    QueryTunnelUtil.encode(req, requestContext, _queryPostThreshold, callback);
  }
}
