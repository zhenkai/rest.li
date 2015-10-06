package com.linkedin.r2.testutils.filter;

import com.linkedin.r2.filter.NextFilter;
import com.linkedin.r2.filter.message.rest.StreamFilter;
import com.linkedin.r2.message.RequestContext;
import com.linkedin.r2.message.stream.StreamRequest;
import com.linkedin.r2.message.stream.StreamResponse;

import java.util.Map;

/**
 * @author Zhenkai Zhu
 */
public class StreamCountFilter implements StreamFilter
{
  private int _reqCount;
  private int _resCount;
  private int _errCount;

  public int getStreamReqCount()
  {
    return _reqCount;
  }

  public int getStreamResCount()
  {
    return _resCount;
  }

  public int getStreamErrCount()
  {
    return _errCount;
  }

  public void reset()
  {
    _reqCount = _resCount = _errCount = 0;
  }

  @Override
  public void onRequest(StreamRequest req, RequestContext requestContext, Map<String, String> wireAttrs,
                        NextFilter<StreamRequest, StreamResponse> nextFilter)
  {
    _reqCount++;
    nextFilter.onRequest(req, requestContext, wireAttrs);
  }

  @Override
  public void onResponse(StreamResponse res, RequestContext requestContext, Map<String, String> wireAttrs,
                         NextFilter<StreamRequest, StreamResponse> nextFilter)
  {
    _resCount++;
    nextFilter.onResponse(res, requestContext, wireAttrs);
  }

  @Override
  public void onError(Throwable ex, RequestContext requestContext, Map<String, String> wireAttrs,
                      NextFilter<StreamRequest, StreamResponse> nextFilter)
  {
    _errCount++;
    nextFilter.onError(ex, requestContext, wireAttrs);
  }
}
