package com.linkedin.r2.transport.common;

import com.linkedin.common.callback.Callback;
import com.linkedin.common.util.None;
import com.linkedin.r2.message.RequestContext;
import com.linkedin.r2.message.rest.RestRequest;
import com.linkedin.r2.message.rest.RestResponse;

/**
 * @author Zhenkai Zhu
 */
public interface StreamClient
{
  /**
   * Asynchronously sends a stream request. The given callback is invoked when the  stream response is received.
   *
   * @param request the stream request
   * @param requestContext context for the request
   * @param callback callback to be called when the stream response is received
   */
  void streamRestRequest(RestRequest request, RequestContext requestContext, Callback<RestResponse> callback);

  /**
   * Initiates asynchronous shutdown of the client. This method should block minimally, if at all.
   *
   * @param callback a callback to invoke when the shutdown is complete
   */
  void shutdown(Callback<None> callback);
}
