package com.linkedin.r2.transport.common;

import com.linkedin.common.callback.Callback;
import com.linkedin.r2.message.RequestContext;
import com.linkedin.r2.message.rest.Messages;
import com.linkedin.r2.message.rest.RestRequest;
import com.linkedin.r2.message.rest.StreamRequest;
import com.linkedin.r2.message.rest.StreamResponse;


/**
 * @author Zhenkai Zhu
 */
public class StreamRequestHandlerAdapter implements StreamRequestHandler
{
  private final RestRequestHandler _restRequestHandler;

  public StreamRequestHandlerAdapter(RestRequestHandler restRequestHandler)
  {
    _restRequestHandler = restRequestHandler;
  }

  @Override
  public void handleRequest(StreamRequest request, final RequestContext requestContext, final Callback<StreamResponse> callback)
  {
    Messages.toRestRequest(request, new Callback<RestRequest>()
    {
      @Override
      public void onError(Throwable e)
      {
        // we cannot use callback because callback is only for response error, but this is error in receiving request
        throw new RuntimeException(e);
      }

      @Override
      public void onSuccess(RestRequest restRequest)
      {
        _restRequestHandler.handleRequest(restRequest, requestContext, Messages.toRestCallback(callback));
      }
    });
  }
}
