package test.r2.integ;

import com.linkedin.common.callback.Callback;
import com.linkedin.r2.filter.R2Constants;
import com.linkedin.r2.message.RequestContext;
import com.linkedin.r2.message.rest.RestRequest;
import com.linkedin.r2.message.rest.RestRequestBuilder;
import com.linkedin.r2.message.rest.RestResponse;
import com.linkedin.r2.message.rest.RestStatus;
import com.linkedin.r2.message.rest.StreamRequest;
import com.linkedin.r2.message.rest.StreamResponse;
import com.linkedin.r2.message.rest.StreamResponseBuilder;
import com.linkedin.r2.message.streaming.EntityStreams;
import com.linkedin.r2.sample.Bootstrap;
import com.linkedin.r2.transport.common.StreamRequestHandler;
import com.linkedin.r2.transport.common.bridge.server.TransportDispatcher;
import com.linkedin.r2.transport.common.bridge.server.TransportDispatcherBuilder;
import com.linkedin.r2.transport.http.client.HttpClientFactory;
import com.linkedin.r2.transport.http.server.HttpJettyServer;
import com.linkedin.r2.transport.http.server.HttpServerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Zhenkai Zhu
 */
public class TestQueryTunnel extends AbstractStreamTest
{
  private static final int THRESHOLD = 16;
  private static URI LONG_QUERY = URI.create("/test?1234239=923929,i=99999");
  private static URI SHORT_QUERY = URI.create("/test?i=1");

  private SimpleHandler _longHandler;
  private SimpleHandler _shortHandler;

  @Override
  protected TransportDispatcher getTransportDispatcher()
  {
    _longHandler = new SimpleHandler();
    _shortHandler = new SimpleHandler();
    return new TransportDispatcherBuilder().addStreamHandler(LONG_QUERY, _longHandler)
        .addStreamHandler(SHORT_QUERY, _shortHandler).build();
  }

  @Test
  public void testShortQuery() throws Exception
  {
    RestRequest restRequest = new RestRequestBuilder(Bootstrap.createHttpURI(PORT, SHORT_QUERY)).build();
    RestResponse response = _client.restRequest(restRequest).get();
    Assert.assertEquals(response.getStatus(), RestStatus.OK);
    Assert.assertFalse(_shortHandler.isQueryTunneled());
  }

  @Test
  public void testLongQuery() throws Exception
  {
    RestRequest restRequest = new RestRequestBuilder(Bootstrap.createHttpURI(PORT, LONG_QUERY)).build();
    RestResponse response = _client.restRequest(restRequest).get();
    Assert.assertEquals(response.getStatus(), RestStatus.OK);
    Assert.assertTrue(_longHandler.isQueryTunneled());
  }

  private static class SimpleHandler implements StreamRequestHandler
  {
    private boolean _isQueryTunneled;

    public void handleRequest(StreamRequest request, RequestContext requestContext, Callback<StreamResponse> callback)
    {
      Object isQueryTunneled = requestContext.getLocalAttr(R2Constants.IS_QUERY_TUNNELED);
      _isQueryTunneled = isQueryTunneled != null && (Boolean)isQueryTunneled;
      callback.onSuccess(new StreamResponseBuilder().build(EntityStreams.emptyStream()));
    }

    public boolean isQueryTunneled()
    {
      return _isQueryTunneled;
    }

  }

  @Override
  protected Map<String, String> getClientProperties()
  {
    Map<String, String> clientProperties = new HashMap<String, String>();
    clientProperties.put(HttpClientFactory.HTTP_QUERY_POST_THRESHOLD, String.valueOf(THRESHOLD));
    return clientProperties;
  }

  protected HttpServerFactory getServerFactory()
  {
    return new HttpServerFactory();
  }
}
