package com.linkedin.r2.transport.common;

import com.linkedin.common.callback.Callback;
import com.linkedin.r2.message.RequestContext;
import com.linkedin.r2.message.rest.RestRequest;
import com.linkedin.r2.message.rest.RestResponse;

/**
 * A handler for stream request which sends back stream response.
 *
 * @author Zhenkai Zhu
 */
public interface StreamResponseHandler
{
  /**
   * Handles the request and invokes the callback when stream response is available.
   *
   * @param request the stream request to be processed
   * @param requestContext the context for the request
   * @param callback the callback to be invoked when the stream response is available.
   */
  void handleStreamRequest(RestRequest request, RequestContext requestContext, Callback<RestResponse> callback);
}
