package test.r2.integ;

import com.linkedin.r2.filter.FilterChains;
import com.linkedin.r2.transport.http.server.HttpJettyServer;
import com.linkedin.r2.transport.http.server.HttpServerFactory;
import org.testng.annotations.Test;


/**
 * @author Ang Xu
 */
@Test
public class TestCompressionEchoAsyncEvent extends TestCompressionEcho
{
  @Override
  protected HttpServerFactory getServerFactory()
  {
    return new HttpServerFactory(FilterChains.create(_compressionFilter), HttpJettyServer.ServletType.ASYNC_EVENT);
  }
}
