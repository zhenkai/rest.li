/*
   Copyright (c) 2015 LinkedIn Corp.

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


import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.util.ssl.SslContextFactory;


/**
 * @author Ang Xu
 * @version $Revision: $
 */
public class HttpsJettyServer extends HttpJettyServer
{
  private final int _sslPort;
  private final String _keyStore;
  private final String _keyStorePassword;

  public HttpsJettyServer(int port,
                          int sslPort,
                          String keyStore,
                          String keyStorePassword,
                          String contextPath,
                          int threadPoolSize,
                          HttpDispatcher dispatcher,
                          boolean useAsync,
                          int asyncTimeOut)
  {
    super(port, contextPath, threadPoolSize, dispatcher, useAsync, asyncTimeOut);
    _sslPort = sslPort;
    _keyStore = keyStore;
    _keyStorePassword = keyStorePassword;
  }

  @Override
  protected Connector[] getConnectors(Server server)
  {
    SslContextFactory sslContextFactory = new SslContextFactory();
    sslContextFactory.setKeyStorePath(_keyStore);
    sslContextFactory.setKeyStorePassword(_keyStorePassword);
    sslContextFactory.setTrustStorePath(_keyStore);
    sslContextFactory.setTrustStorePassword(_keyStorePassword);

    SslConnectionFactory sslFactory = new SslConnectionFactory(sslContextFactory, "http/1.1");
    ServerConnector https = new ServerConnector(server, sslFactory, new HttpConnectionFactory());
    https.setPort(_sslPort);

    Connector[] httpConnectors = super.getConnectors(server);
    Connector[] connectors = new Connector[httpConnectors.length + 1];
    int i  = 0;
    for (Connector c : httpConnectors)
    {
      connectors[i++] = c;
    }
    connectors[i++] = https;

    return connectors;
  }
}
