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

/**
 * $Id: $
 */

package com.linkedin.r2.transport.http.server;

import com.linkedin.r2.transport.common.bridge.server.TransportDispatcher;

/**
 * @author Steven Ihde
 * @version $Revision: $
 */

public class RAPServlet extends AbstractR2Servlet
{
  private static final long serialVersionUID = 0L;
  private static final long DEFAULT_IOHANDLER_TIMEOUT = 30000;

  private final HttpDispatcher _dispatcher;

  public RAPServlet(HttpDispatcher dispatcher)
  {
    this(dispatcher, DEFAULT_IOHANDLER_TIMEOUT);
  }

  public RAPServlet(TransportDispatcher dispatcher)
  {
    this(new HttpDispatcher(dispatcher));
  }

  public RAPServlet(TransportDispatcher dispatcher, long ioHandlerTimeout)
  {
    this(new HttpDispatcher(dispatcher), ioHandlerTimeout);
  }

  public RAPServlet(HttpDispatcher dispatcher, long ioHandlerTimeout)
  {
    super(ioHandlerTimeout);
    _dispatcher = dispatcher;
  }

  @Override
  protected HttpDispatcher getDispatcher()
  {
    return _dispatcher;
  }
}
