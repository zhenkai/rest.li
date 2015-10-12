/*
   Copyright (c) 2012 LinkedIn Corp.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

/* $Id$ */
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
  private final Map<URI, StreamRequestHandler> _streamHandlers;
  private final Map<URI, RestRequestHandler> _restHandlers;

  public TransportDispatcherBuilder()
  {
    this(new HashMap<URI, RestRequestHandler>(), new HashMap<URI, StreamRequestHandler>());
  }

  public TransportDispatcherBuilder(Map<URI, RestRequestHandler> restHandlers, Map<URI, StreamRequestHandler> streamHandlers)
  {
    _restHandlers = new HashMap<URI, RestRequestHandler>(restHandlers);
    _streamHandlers = new HashMap<URI, StreamRequestHandler>(streamHandlers);
  }

  public TransportDispatcherBuilder addRestHandler(URI uri, RestRequestHandler handler)
  {
    _restHandlers.put(uri, handler);
    return this;
  }

  public RestRequestHandler removeRestHandler(URI uri)
  {
    return _restHandlers.remove(uri);
  }

  public TransportDispatcherBuilder addStreamHandler(URI uri, StreamRequestHandler handler)
  {
    _streamHandlers.put(uri, handler);
    return this;
  }

  public StreamRequestHandler removeStreamHandler(URI uri)
  {
    return _streamHandlers.remove(uri);
  }


  public TransportDispatcherBuilder reset()
  {
    _restHandlers.clear();
    _streamHandlers.clear();
    return this;
  }

  public TransportDispatcher build()
  {
    return new TransportDispatcherImpl(_restHandlers, _streamHandlers);
  }

}
