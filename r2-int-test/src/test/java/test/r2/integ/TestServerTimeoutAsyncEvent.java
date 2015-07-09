package test.r2.integ;

import com.linkedin.common.callback.Callback;
import com.linkedin.common.callback.FutureCallback;
import com.linkedin.common.util.None;
import com.linkedin.data.ByteString;
import com.linkedin.r2.message.RequestContext;
import com.linkedin.r2.message.rest.RestException;
import com.linkedin.r2.message.rest.RestRequestBuilder;
import com.linkedin.r2.message.rest.RestResponse;
import com.linkedin.r2.message.rest.RestStatus;
import com.linkedin.r2.message.rest.StreamRequest;
import com.linkedin.r2.message.rest.StreamRequestBuilder;
import com.linkedin.r2.message.rest.StreamResponse;
import com.linkedin.r2.message.rest.StreamResponseBuilder;
import com.linkedin.r2.message.streaming.DrainReader;
import com.linkedin.r2.message.streaming.EntityStreams;
import com.linkedin.r2.message.streaming.ReadHandle;
import com.linkedin.r2.message.streaming.Reader;
import com.linkedin.r2.message.streaming.WriteHandle;
import com.linkedin.r2.message.streaming.Writer;
import com.linkedin.r2.sample.Bootstrap;
import com.linkedin.r2.transport.common.Client;
import com.linkedin.r2.transport.common.StreamRequestHandler;
import com.linkedin.r2.transport.common.bridge.client.TransportClientAdapter;
import com.linkedin.r2.transport.common.bridge.common.TransportCallback;
import com.linkedin.r2.transport.common.bridge.common.TransportResponseImpl;
import com.linkedin.r2.transport.common.bridge.server.TransportCallbackAdapter;
import com.linkedin.r2.transport.common.bridge.server.TransportDispatcher;
import com.linkedin.r2.transport.common.bridge.server.TransportDispatcherBuilder;
import com.linkedin.r2.transport.http.client.HttpClientFactory;
import com.linkedin.r2.transport.http.server.HttpJettyServer;
import com.linkedin.r2.transport.http.server.HttpServer;
import com.linkedin.r2.transport.http.server.HttpServerFactory;
import junit.framework.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * @author Zhenkai Zhu
 */
public class TestServerTimeoutAsyncEvent
{
  private static final int PORT = 10001;
  private static final URI TIMEOUT_BEFORE_SENDING_RESPONSE_SERVER_URI = URI.create("/timeout-before-sending-response");
  private static final URI TIMEOUT_AFTER_SENDING_RESPONSE_SERVER_URI = URI.create("/timeout-after-sending-response");
  private static final URI THROW_BUT_SHOULD_NOT_TIMEOUT_URI = URI.create("/throw-but-should-not-timeout");
  private static final int ASYNC_EVENT_TIMEOUT = 500;
  private static final int RESPONSE_SIZE_WRITTEN_SO_FAR = 50 * 1024;
  private HttpClientFactory _clientFactory;
  private Client _client;
  private HttpServer _server;
  private ExecutorService _asyncExecutor;

  @BeforeClass
  public void setup() throws IOException
  {
    _clientFactory = new HttpClientFactory();
    Map<String, Object> clientProperties = new HashMap<String, Object>();
    clientProperties.put(HttpClientFactory.HTTP_REQUEST_TIMEOUT, String.valueOf(ASYNC_EVENT_TIMEOUT * 20));
    _client = new TransportClientAdapter(_clientFactory.getClient(clientProperties));
    final Map<URI, StreamRequestHandler> handlers = new HashMap<URI, StreamRequestHandler>();
    handlers.put(TIMEOUT_BEFORE_SENDING_RESPONSE_SERVER_URI, new TimeoutBeforeRespondingRequestHandler());
    handlers.put(TIMEOUT_AFTER_SENDING_RESPONSE_SERVER_URI, new TimeoutAfterRespondingRequestHandler());
    handlers.put(THROW_BUT_SHOULD_NOT_TIMEOUT_URI, new ThrowHandler());
    TransportDispatcher transportDispatcher = new TransportDispatcher()
    {
      @Override
      public void handleStreamRequest(StreamRequest req, Map<String, String> wireAttrs, RequestContext requestContext, TransportCallback<StreamResponse> callback)
      {
        StreamRequestHandler handler = handlers.get(req.getURI());
        if (handler != null)
        {
          handler.handleRequest(req, requestContext, new TransportCallbackAdapter<StreamResponse>(callback));
        }
        else
        {
          req.getEntityStream().setReader(new DrainReader());
          callback.onResponse(TransportResponseImpl.<StreamResponse>error(new IllegalStateException("Handler not found for URI " + req.getURI())));
        }
      }
    };
    _server = new HttpServerFactory(HttpJettyServer.ServletType.ASYNC_EVENT).createServer(PORT, transportDispatcher, ASYNC_EVENT_TIMEOUT);
    _server.start();
    _asyncExecutor = Executors.newSingleThreadExecutor();
  }

  @Test
  public void testServerTimeoutAfterResponding() throws Exception
  {
    Future<RestResponse> futureResponse =
        _client.restRequest(new RestRequestBuilder(Bootstrap.createHttpURI(PORT, TIMEOUT_AFTER_SENDING_RESPONSE_SERVER_URI)).build());

    // server should timeout so get should succeed
    RestResponse response = futureResponse.get(ASYNC_EVENT_TIMEOUT * 2, TimeUnit.MILLISECONDS);
    Assert.assertEquals(response.getStatus(), RestStatus.OK);
    Assert.assertEquals(response.getEntity().length(), RESPONSE_SIZE_WRITTEN_SO_FAR);
  }

  @Test
  public void testServerTimeoutBeforeResponding() throws Exception
  {
    Future<RestResponse> futureResponse =
        _client.restRequest(new RestRequestBuilder(Bootstrap.createHttpURI(PORT, TIMEOUT_BEFORE_SENDING_RESPONSE_SERVER_URI)).build());

    try
    {
      futureResponse.get(ASYNC_EVENT_TIMEOUT * 2, TimeUnit.MILLISECONDS);
      Assert.fail("Should have thrown exception");
    }
    catch (ExecutionException ex)
    {
      Throwable cause = ex.getCause();
      Assert.assertNotNull(cause);
      Assert.assertTrue(cause instanceof RestException);
      RestException restException = (RestException) cause;
      Assert.assertEquals(restException.getResponse().getStatus(), RestStatus.INTERNAL_SERVER_ERROR);
      Assert.assertEquals(restException.getResponse().getEntity().asString("UTF8"), "Server timeout");
    }
  }

  // this test will hang on shutdown if not canceling the request stream in DispatcherRequestFilter when StreamRequestHandler throws
  @Test
  public void testServerThrowButShouldNotTimeout() throws Exception
  {
    final CountDownLatch latch = new CountDownLatch(2);
    BytesWriter writer = new BytesWriter(2000 * 1024, (byte)100) {
      @Override
      protected void onFinish()
      {
        latch.countDown();
      }
    };
    StreamRequest request = new StreamRequestBuilder(Bootstrap.createHttpURI(PORT, THROW_BUT_SHOULD_NOT_TIMEOUT_URI)).build(EntityStreams.newEntityStream(writer));
    _client.streamRequest(request, new Callback<StreamResponse>()
    {
      @Override
      public void onError(Throwable e)
      {
        latch.countDown();
      }

      @Override
      public void onSuccess(StreamResponse result)
      {
        latch.countDown();
      }
    });

    Assert.assertTrue(latch.await(ASYNC_EVENT_TIMEOUT * 2, TimeUnit.MILLISECONDS));
    // Server shouldn't be waiting for the request to finish, which causes timeout
    // R2 code should have helped to drain the request
    Assert.assertTrue(writer.isDone());
  }

  @AfterClass
  public void tearDown() throws Exception
  {

    final FutureCallback<None> clientShutdownCallback = new FutureCallback<None>();
    _client.shutdown(clientShutdownCallback);
    clientShutdownCallback.get();

    final FutureCallback<None> factoryShutdownCallback = new FutureCallback<None>();
    _clientFactory.shutdown(factoryShutdownCallback);
    factoryShutdownCallback.get();

    if (_server != null) {
      _server.stop();
      _server.waitForStop();
    }
    _asyncExecutor.shutdown();
  }

  private class ThrowHandler implements StreamRequestHandler
  {
    @Override
    public void handleRequest(final StreamRequest request, RequestContext requestContext, Callback<StreamResponse> callback)
    {
      throw new RuntimeException("Throw for test.");
    }
  }

  private class TimeoutBeforeRespondingRequestHandler implements StreamRequestHandler
  {
    @Override
    public void handleRequest(final StreamRequest request, RequestContext requestContext, Callback<StreamResponse> callback)
    {
      _asyncExecutor.execute(new Runnable()
      {
        @Override
        public void run()
        {
          request.getEntityStream().setReader(new Reader()
          {
            @Override
            public void onInit(ReadHandle rh)
            {

            }

            @Override
            public void onDataAvailable(ByteString data)
            {

            }

            @Override
            public void onDone()
            {

            }

            @Override
            public void onError(Throwable e)
            {

            }
          });
        }
      });
    }
  }

  private class TimeoutAfterRespondingRequestHandler implements StreamRequestHandler
  {
    @Override
    public void handleRequest(final StreamRequest request, RequestContext requestContext, final Callback<StreamResponse> callback)
    {
      _asyncExecutor.execute(new Runnable()
      {
        @Override
        public void run()
        {
          request.getEntityStream().setReader(new DrainReader());
          Writer noFinishWriter = new Writer()
          {
            private WriteHandle _wh;
            boolean _written = false;

            @Override
            public void onInit(WriteHandle wh)
            {
              _wh = wh;
            }

            @Override
            public void onWritePossible()
            {
              if (!_written)
              {
                _wh.write(ByteString.copy(new byte[RESPONSE_SIZE_WRITTEN_SO_FAR]));
              }
            }

            @Override
            public void onAbort(Throwable e)
            {

            }
          };

          callback.onSuccess(new StreamResponseBuilder().build(EntityStreams.newEntityStream(noFinishWriter)));
        }
      });

    }
  }
}
