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

package test.r2.integ;

import com.linkedin.r2.sample.Bootstrap;
import com.linkedin.r2.sample.echo.EchoService;
import com.linkedin.r2.sample.echo.rest.RestEchoClient;
import com.linkedin.r2.transport.common.Client;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

import java.net.URI;

/**
 * @author Chris Pettitt
 * @version $Revision: $
 */

@Test
public class TestHttpRestEcho extends AbstractHttpEchoServiceTest
{
  private static final int PORT = 11996;

  private final int _port;

  @Factory(dataProvider = "configs")
  public TestHttpRestEcho(boolean clientROS, boolean serverROS, int port)
  {
    super(clientROS, serverROS, port);
    _port = port;
  }

  @DataProvider
  public static Object[][] configs()
  {
    return new Object[][] {
        {true, true, PORT},
        {true, false, PORT + 1},
        {false, true, PORT + 2},
        {false, false, PORT + 3}
    };
  }

  @Override
  protected EchoService getEchoClient(Client client, URI uri)
  {
    return new RestEchoClient(Bootstrap.createHttpURI(_port, uri), client);
  }
}
