package test.r2.integ;

import com.linkedin.r2.transport.http.server.HttpServerFactory;

/**
 * @author Zhenkai Zhu
 */
public class TestStreamResponseJetty9 extends TestStreamResponse
{
  @Override
  protected HttpServerFactory getServerFactory()
  {
    return new HttpServerFactory(true);
  }
}
