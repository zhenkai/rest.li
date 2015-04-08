package com.linkedin.r2.transport.common.bridge.server;

import com.linkedin.r2.message.RequestContext;
import com.linkedin.r2.message.rest.StreamRequest;
import com.linkedin.r2.message.rest.StreamResponse;
import com.linkedin.r2.transport.common.bridge.common.TransportCallback;

import java.util.Map;

/**
 * @author Zhenkai Zhu
 */
public interface TransportDispatcher
{
  /**
   * Dispatch a {@link com.linkedin.r2.message.rest.StreamRequest}.
   *
   * @param req the {@link com.linkedin.r2.message.rest.StreamRequest} to be dispatched.
   * @param wireAttrs the wire attributes of the request.
   * @param requestContext {@link com.linkedin.r2.message.RequestContext} context for the request
   * @param callback a {@link com.linkedin.r2.transport.common.bridge.common.TransportCallback} to be called with the {@link com.linkedin.r2.message.rest.RestResponse}.
   */
   void handleStreamRequest(StreamRequest req, Map<String, String> wireAttrs,
                         RequestContext requestContext, TransportCallback<StreamResponse> callback);
}
