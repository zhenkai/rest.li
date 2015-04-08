package test.r2.integ;

import com.linkedin.r2.transport.http.server.HttpJettyServer;
import com.linkedin.r2.transport.http.server.HttpServerFactory;
import org.testng.annotations.Test;

/**
 * @author Zhenkai Zhu
 */
public class TestStreamRequestAsyncEvent extends TestStreamRequest
{
  @Override
  protected HttpServerFactory getServerFactory()
  {
    return new HttpServerFactory(HttpJettyServer.ServletType.ASYNC_EVENT);
  }

  @Test
  public void testBackPressure() throws Exception
  {
    super.testBackPressure();
  }

  @Test
  public void testRequestLarge() throws Exception
  {
    super.testRequestLarge();
  }

  @Test
  public void test404() throws Exception
  {
    super.test404();
  }
}
