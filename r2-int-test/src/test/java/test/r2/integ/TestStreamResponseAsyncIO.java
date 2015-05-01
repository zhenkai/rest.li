package test.r2.integ;

import com.linkedin.r2.transport.http.server.HttpJettyServer;
import com.linkedin.r2.transport.http.server.HttpServerFactory;
import org.testng.annotations.Test;

/**
 * @author Zhenkai Zhu
 */
@Test
public class TestStreamResponseAsyncIO  extends TestStreamResponse  // disable class didn't work
{
  protected HttpServerFactory getServerFactory()
  {
    return new HttpServerFactory(HttpJettyServer.ServletType.ASYNC_IO);
  }
}
