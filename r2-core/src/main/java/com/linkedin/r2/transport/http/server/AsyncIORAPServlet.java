package com.linkedin.r2.transport.http.server;

import com.linkedin.r2.transport.common.bridge.server.TransportDispatcher;

/**
 * WARNING: THIS CLASS IS EXPERIMENTAL AND HAS UNRESOLVED BUGS. DO NOT USE IN PROD.
 *
 * @author Zhenkai Zhu
 */
public class AsyncIORAPServlet extends AbstractAsyncIOR2Servlet
{
  private static final long serialVersionUID = 0L;
  private static final int DEFAULT_TIMEOUT = 30000;

  private final HttpDispatcher _dispatcher;

  public AsyncIORAPServlet(HttpDispatcher dispatcher, int timeout)
  {
    super(timeout);
    _dispatcher = dispatcher;
  }

  public AsyncIORAPServlet(TransportDispatcher dispatcher, int timeout)
  {
    this(new HttpDispatcher(dispatcher), timeout);
  }

  public AsyncIORAPServlet(HttpDispatcher dispatcher)
  {
    this(dispatcher, DEFAULT_TIMEOUT);
  }

  public AsyncIORAPServlet(TransportDispatcher dispatcher)
  {
    this(new HttpDispatcher(dispatcher));
  }

  @Override
  protected HttpDispatcher getDispatcher()
  {
    return _dispatcher;
  }
}
