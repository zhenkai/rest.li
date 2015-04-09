package com.linkedin.r2.transport.common.bridge.server;

import com.linkedin.r2.transport.common.RestRequestHandler;
import com.linkedin.r2.transport.common.StreamRequestHandler;
import com.linkedin.r2.transport.common.StreamRequestHandlerAdapter;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Zhenkai Zhu
 */
public class TransportDispatcherBuilder
{
  private final Map<URI, StreamRequestHandler> _streamHandlers = new HashMap<URI, StreamRequestHandler>();

  public TransportDispatcherBuilder addStreamHandler(URI uri, StreamRequestHandler handler)
  {
    _streamHandlers.put(uri, handler);
    return this;
  }

  public TransportDispatcherBuilder addRestHandler(URI uri, RestRequestHandler handler)
  {
    _streamHandlers.put(uri, new StreamRequestHandlerAdapter(handler));
    return this;
  }

  public StreamRequestHandler removeStreamHandler(URI uri)
  {
    return _streamHandlers.remove(uri);
  }


  public TransportDispatcherBuilder reset()
  {
    _streamHandlers.clear();
    return this;
  }

  public TransportDispatcher build()
  {
    return new TransportDispatcherImpl(new HashMap<URI, StreamRequestHandler>(_streamHandlers));
  }

}
