package test.r2.integ;

import com.linkedin.r2.transport.http.server.HttpServerFactory;

/**
 * @author Zhenkai Zhu
 */
public class TestStreamEchoJetty9 extends TestStreamEcho
{
  @Override
  protected HttpServerFactory getServerFactory()
  {
    return new HttpServerFactory(true);
  }
}
