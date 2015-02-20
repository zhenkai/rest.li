package com.linkedin.r2.transport.common.bridge.server;

import com.linkedin.r2.transport.common.StreamRequestHandler;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Zhenkai Zhu
 */
public class StreamDispatcherBuilder
{
  private final Map<URI, StreamRequestHandler> _streamHandlers = new HashMap<URI, StreamRequestHandler>();

  public StreamDispatcherBuilder addStreamHandler(URI uri, StreamRequestHandler handler)
  {
    _streamHandlers.put(uri, handler);
    return this;
  }

  public StreamRequestHandler removeStreamHandler(URI uri)
  {
    return _streamHandlers.remove(uri);
  }


  public StreamDispatcherBuilder reset()
  {
    _streamHandlers.clear();
    return this;
  }

  public StreamDispatcher build()
  {
    return new StreamDispatcherImpl(new HashMap<URI, StreamRequestHandler>(_streamHandlers));
  }

}
