package test.r2.integ;

import com.linkedin.r2.transport.http.server.HttpJettyServer;
import com.linkedin.r2.transport.http.server.HttpServerFactory;
import org.testng.annotations.Test;

/**
 * @author Zhenkai Zhu
 */
public class TestStreamResponseAsyncEvent extends TestStreamResponse
{
  @Override
  protected HttpServerFactory getServerFactory()
  {
    return new HttpServerFactory(HttpJettyServer.ServletType.ASYNC_EVENT);
  }

  @Test
  public void testErrorWhileStreaming() throws Exception
  {
    super.testErrorWhileStreaming();
  }
}
