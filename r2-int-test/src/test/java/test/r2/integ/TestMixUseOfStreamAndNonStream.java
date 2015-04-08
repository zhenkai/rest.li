package test.r2.integ;

import com.linkedin.common.callback.Callback;
import com.linkedin.common.callback.FutureCallback;
import com.linkedin.common.util.None;
import com.linkedin.r2.message.RequestContext;
import com.linkedin.r2.message.rest.Messages;
import com.linkedin.r2.message.rest.RestRequest;
import com.linkedin.r2.message.rest.RestRequestBuilder;
import com.linkedin.r2.message.rest.RestResponse;
import com.linkedin.r2.message.rest.RestResponseBuilder;
import com.linkedin.r2.message.rest.RestStatus;
import com.linkedin.r2.message.rest.StreamRequest;
import com.linkedin.r2.message.rest.StreamResponse;
import com.linkedin.r2.sample.Bootstrap;
import com.linkedin.r2.transport.common.Client;
import com.linkedin.r2.transport.common.RestRequestHandler;
import com.linkedin.r2.transport.common.StreamRequestHandler;
import com.linkedin.r2.transport.common.StreamRequestHandlerAdapter;
import com.linkedin.r2.transport.common.bridge.client.TransportClientAdapter;
import com.linkedin.r2.transport.common.bridge.common.TransportCallback;
import com.linkedin.r2.transport.common.bridge.server.TransportDispatcher;
import com.linkedin.r2.transport.common.bridge.server.TransportCallbackAdapter;
import com.linkedin.r2.transport.http.client.HttpClientFactory;
import com.linkedin.r2.transport.http.server.HttpServer;
import com.linkedin.r2.transport.http.server.HttpServerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Zhenkai Zhu
 */
public class TestMixUseOfStreamAndNonStream
{
  private HttpClientFactory _clientFactory;
  private static final int PORT = 8098;
  private static final String REST_MESSAGE = "This is from rest handler";
  private static final String STREAM_MESSAGE = "This is from stream handler";
  private static final URI REST_RESOURCE_URI = URI.create("/rest");
  private static final URI STREAM_RESOURCE_URI = URI.create("/stream");

  private HttpServer _server;
  private Client _client;

  @BeforeSuite
  public void setup() throws IOException
  {
    _clientFactory = new HttpClientFactory();
    final Map<String, String> clientProperties = new HashMap<String, String>();
    _client = new TransportClientAdapter(_clientFactory.getClient(clientProperties));

    final RestHandler restHandler = new RestHandler();
    final StreamRequestHandler adaptedHandler = new StreamRequestHandlerAdapter(restHandler);
    final StreamHandler streamHandler = new StreamHandler();
    final TransportDispatcher dispatcher = new TransportDispatcher()
    {
      @Override
      public void handleStreamRequest(StreamRequest req, Map<String, String> wireAttrs, RequestContext requestContext, TransportCallback<StreamResponse> callback)
      {
        if (req.getURI().equals(REST_RESOURCE_URI))
        {
          adaptedHandler.handleRequest(req, requestContext, new TransportCallbackAdapter<StreamResponse>(callback));
        }
        else
        {
          streamHandler.handleRequest(req, requestContext, new TransportCallbackAdapter<StreamResponse>(callback));
        }
      }
    };

    _server = new HttpServerFactory().createServer(PORT, dispatcher);
    _server.start();
  }

  @AfterSuite
  public void tearDown() throws Exception
  {
    if (_server != null) {
      _server.stop();
      _server.waitForStop();
    }
    final FutureCallback<None> clientCallback = new FutureCallback<None>();
    _client.shutdown(clientCallback);
    clientCallback.get();

    final FutureCallback<None> factoryCallback = new FutureCallback<None>();
    _clientFactory.shutdown(factoryCallback);
    factoryCallback.get();
  }

  @Test
  public void testRest() throws Exception
  {
    RestRequest restRequest = new RestRequestBuilder(Bootstrap.createHttpURI(PORT, REST_RESOURCE_URI)).build();
    RestResponse response = _client.restRequest(restRequest).get();
    Assert.assertEquals(response.getEntity().asString(Charset.defaultCharset()), REST_MESSAGE);
  }

  @Test
  public void testStream() throws Exception
  {
    RestRequest restRequest = new RestRequestBuilder(Bootstrap.createHttpURI(PORT, STREAM_RESOURCE_URI)).build();
    RestResponse response = _client.restRequest(restRequest).get();
    Assert.assertEquals(response.getEntity().asString(Charset.defaultCharset()), STREAM_MESSAGE);
  }

  private static class RestHandler implements RestRequestHandler
  {
    @Override
    public void handleRequest(RestRequest request, RequestContext requestContext, Callback<RestResponse> callback)
    {
      RestResponseBuilder builder = new RestResponseBuilder();
      builder.setStatus(RestStatus.OK);
      builder.setEntity(REST_MESSAGE.getBytes());
      RestResponse response = builder.build();
      callback.onSuccess(response);
    }
  }

  private static class StreamHandler implements StreamRequestHandler
  {
    @Override
    public void handleRequest(StreamRequest request, RequestContext requestContext, Callback<StreamResponse> callback)
    {
      RestResponseBuilder builder = new RestResponseBuilder();
      builder.setStatus(RestStatus.OK);
      builder.setEntity(STREAM_MESSAGE.getBytes());
      RestResponse response = builder.build();
      callback.onSuccess(Messages.toStreamResponse(response));
    }
  }
}
