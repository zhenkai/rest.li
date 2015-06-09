package test.r2.integ;

import com.linkedin.common.callback.Callback;
import com.linkedin.r2.message.RequestContext;
import com.linkedin.r2.message.rest.RestRequest;
import com.linkedin.r2.message.rest.RestRequestBuilder;
import com.linkedin.r2.message.rest.RestResponse;
import com.linkedin.r2.message.rest.RestResponseBuilder;
import com.linkedin.r2.sample.Bootstrap;
import com.linkedin.r2.transport.common.RestRequestHandler;
import com.linkedin.r2.transport.common.StreamRequestHandler;
import com.linkedin.r2.transport.common.StreamRequestHandlerAdapter;
import com.linkedin.r2.transport.http.server.HttpJettyServer;
import com.linkedin.r2.transport.http.server.HttpServerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.net.URI;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Zhenkai Zhu
 */
public class TestStreamEchoAsyncEvent extends TestStreamEcho
{
  private static final URI DELAYED_ECHO_URI = URI.create("/delayed-echo");

  @Override
  protected HttpServerFactory getServerFactory()
  {
    return new HttpServerFactory(HttpJettyServer.ServletType.ASYNC_EVENT);
  }

  @Test
  public void testDelayedEcho() throws Exception
  {
    RestRequest restRequest = new RestRequestBuilder(Bootstrap.createHttpURI(PORT, DELAYED_ECHO_URI))
        .setEntity("wei ni hao ma?".getBytes()).build();
    RestResponse response = _client.restRequest(restRequest).get();
    Assert.assertEquals(response.getEntity().asString(Charset.defaultCharset()), "wei ni hao ma?");
  }

  @Override
  protected Map<URI, StreamRequestHandler> getHandlers()
  {
    Map<URI, StreamRequestHandler> handlers = new HashMap<URI, StreamRequestHandler>(super.getHandlers());
    handlers.put(DELAYED_ECHO_URI, new StreamRequestHandlerAdapter(new DelayedStoreAndForwardEchoHandler()));
    return handlers;
  }

  private class DelayedStoreAndForwardEchoHandler implements RestRequestHandler
  {
    @Override
    public void handleRequest(final RestRequest request, RequestContext requestContext, final Callback<RestResponse> callback)
    {
//      _scheduler.schedule(new Runnable()
//      {
//        @Override
//        public void run()
//        {
//          RestResponse restResponse = new RestResponseBuilder().setEntity(request.getEntity()).build();
//          callback.onSuccess(restResponse);
//        }
//      }, 5000, TimeUnit.MILLISECONDS);
          RestResponse restResponse = new RestResponseBuilder().setEntity(request.getEntity()).build();
          callback.onSuccess(restResponse);
    }
  }

}
