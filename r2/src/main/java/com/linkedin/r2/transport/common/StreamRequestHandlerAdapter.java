package com.linkedin.r2.transport.common;

import com.linkedin.common.callback.Callback;
import com.linkedin.data.ByteString;
import com.linkedin.r2.message.RequestContext;
import com.linkedin.r2.message.rest.Messages;
import com.linkedin.r2.message.rest.RestException;
import com.linkedin.r2.message.rest.RestRequest;
import com.linkedin.r2.message.rest.RestResponse;
import com.linkedin.r2.message.rest.RestStatus;
import com.linkedin.r2.message.rest.StreamRequest;
import com.linkedin.r2.message.rest.StreamResponse;
import com.linkedin.r2.message.rest.StreamResponseBuilder;
import com.linkedin.r2.message.streaming.ByteStringWriter;
import com.linkedin.r2.message.streaming.EntityStreams;


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
        // this mostly likely would not happen as we'd fail already before reaching here
        RestResponse restResponse =
            RestStatus.responseForStatus(RestStatus.INTERNAL_SERVER_ERROR, e.toString());

        callback.onSuccess(Messages.toStreamResponse(restResponse));
      }

      @Override
      public void onSuccess(RestRequest restRequest)
      {
        _restRequestHandler.handleRequest(restRequest, requestContext, Messages.toRestCallback(callback));
      }
    });
  }
}
