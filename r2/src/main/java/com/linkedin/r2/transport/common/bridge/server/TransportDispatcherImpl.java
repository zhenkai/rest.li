package com.linkedin.r2.transport.common.bridge.server;

import com.linkedin.r2.message.RequestContext;
import com.linkedin.r2.message.rest.RestException;
import com.linkedin.r2.message.rest.RestResponse;
import com.linkedin.r2.message.rest.RestStatus;
import com.linkedin.r2.message.rest.StreamRequest;
import com.linkedin.r2.message.rest.StreamResponse;
import com.linkedin.r2.message.streaming.DrainReader;
import com.linkedin.r2.transport.common.StreamRequestHandler;
import com.linkedin.r2.transport.common.bridge.common.TransportCallback;
import com.linkedin.r2.transport.common.bridge.common.TransportResponseImpl;

import java.net.URI;
import java.util.Map;

/**
 * @author Zhenkai Zhu
 */
/* package private */ final class TransportDispatcherImpl implements TransportDispatcher
{
  private final Map<URI, StreamRequestHandler> _streamHandlers;

  /* package private */ TransportDispatcherImpl(Map<URI, StreamRequestHandler> streamHandlers)
  {
    _streamHandlers = streamHandlers;
  }

  @Override
  public void handleStreamRequest(StreamRequest req, Map<String, String> wireAttrs,
                                  RequestContext requestContext,
                                  TransportCallback<StreamResponse> callback)
  {
    final URI address = req.getURI();
    final StreamRequestHandler handler = _streamHandlers.get(address);

    if (handler == null)
    {
      final RestResponse response =
          RestStatus.responseForStatus(RestStatus.NOT_FOUND, "No resource for URI: " + address);
      callback.onResponse(TransportResponseImpl.success((StreamResponse)response));
      req.getEntityStream().setReader(new DrainReader());
      return;
    }

    try
    {
      handler.handleRequest(req, requestContext, new TransportCallbackAdapter<StreamResponse>(callback));
    }
    catch (Exception e)
    {
      final Exception ex = RestException.forError(RestStatus.INTERNAL_SERVER_ERROR, e);
      callback.onResponse(TransportResponseImpl.<StreamResponse>error(ex));
    }
  }

}
