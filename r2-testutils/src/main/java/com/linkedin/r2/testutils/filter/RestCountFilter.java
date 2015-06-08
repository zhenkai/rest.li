package com.linkedin.r2.testutils.filter;

import com.linkedin.r2.filter.NextFilter;
import com.linkedin.r2.filter.message.rest.RestFilter;
import com.linkedin.r2.message.RequestContext;
import com.linkedin.r2.message.rest.RestRequest;
import com.linkedin.r2.message.rest.RestResponse;

import java.util.Map;

/**
 * @auther Zhenkai Zhu
 */

public class RestCountFilter implements RestFilter
{
  private int _reqCount;
  private int _resCount;
  private int _errCount;

  public int getRestReqCount()
  {
    return _reqCount;
  }

  public int getRestResCount()
  {
    return _resCount;
  }

  public int getRestErrCount()
  {
    return _errCount;
  }

  public void reset()
  {
    _reqCount = _resCount = _errCount = 0;
  }

  @Override
  public void onRestRequest(RestRequest req, RequestContext requestContext, Map<String, String> wireAttrs,
                        NextFilter<RestRequest, RestResponse> nextFilter)
  {
    _reqCount++;
    nextFilter.onRequest(req, requestContext, wireAttrs);
  }

  @Override
  public void onRestResponse(RestResponse res, RequestContext requestContext, Map<String, String> wireAttrs,
                         NextFilter<RestRequest, RestResponse> nextFilter)
  {
    _resCount++;
    nextFilter.onResponse(res, requestContext, wireAttrs);
  }

  @Override
  public void onRestError(Throwable ex, RequestContext requestContext, Map<String, String> wireAttrs,
                      NextFilter<RestRequest, RestResponse> nextFilter)
  {
    _errCount++;
    nextFilter.onError(ex, requestContext, wireAttrs);
  }
}
