package test.r2.integ;

import com.linkedin.r2.filter.FilterChains;
import com.linkedin.r2.transport.http.server.HttpJettyServer;
import com.linkedin.r2.transport.http.server.HttpServerFactory;
import org.testng.annotations.Test;


/**
 * DISABLED UNTIL WE FIX THE BUGS.
 *
 * @author Ang Xu
 */
public class TestCompressionEchoAsyncIO //extends TestCompressionEcho
{
//  protected HttpServerFactory getServerFactory()
//  {
//    return new HttpServerFactory(FilterChains.create(_compressionFilter), HttpJettyServer.ServletType.ASYNC_IO);
//  }
}
