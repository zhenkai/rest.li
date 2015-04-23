package test.r2.integ;

import com.linkedin.r2.transport.http.server.HttpJettyServer;
import com.linkedin.r2.transport.http.server.HttpServerFactory;
import org.testng.annotations.Test;

/**
 * @author Zhenkai Zhu
 */
public class TestHttpServerAsyncEvent extends TestHttpServer
{
  protected HttpServerFactory getServerFactory()
  {
    return new HttpServerFactory(HttpJettyServer.ServletType.ASYNC_EVENT);
  }

  @Test
  public void testSuccess() throws Exception
  {
    super.testSuccess();
  }

  @Test
  public void testPost() throws Exception
  {
    super.testPost();
  }

  @Test
  public void testException() throws Exception
  {
    super.testException();
  }

}
