package test.r2.integ;

import com.linkedin.common.callback.Callback;
import com.linkedin.common.callback.FutureCallback;
import com.linkedin.common.util.None;
import com.linkedin.data.ByteString;
import com.linkedin.r2.message.RequestContext;
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

import com.linkedin.r2.transport.common.bridge.server.TransportDispatcher;
import com.linkedin.r2.transport.common.bridge.server.TransportDispatcherBuilder;
import com.linkedin.r2.transport.http.client.HttpClientFactory;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * @author Zhenkai Zhu
 */
public class TestServerTimeout
{
  private static final int PORT = 10001;
  private static final URI BUGGY_SERVER_URI = URI.create("/buggy");
  private static final int SERVER_IOHANDLER_TIMEOUT = 500;
  private HttpClientFactory _clientFactory;
  private Client _client;
  private HttpServer _server;

  @BeforeClass
  public void setup() throws IOException
  {
    _clientFactory = new HttpClientFactory();
    Map<String, Object> clientProperties = new HashMap<String, Object>();
    clientProperties.put(HttpClientFactory.HTTP_REQUEST_TIMEOUT, String.valueOf(SERVER_IOHANDLER_TIMEOUT * 20));
    _client = new TransportClientAdapter(_clientFactory.getClient(clientProperties));
    Map<URI, StreamRequestHandler> handlers = new HashMap<URI, StreamRequestHandler>();
    handlers.put(BUGGY_SERVER_URI, new BuggyRequestHandler());
    TransportDispatcher transportDispatcher = new TransportDispatcherBuilder(handlers).build();
    _server = new HttpServerFactory().createRAPServer(PORT, transportDispatcher, SERVER_IOHANDLER_TIMEOUT);
    _server.start();
  }

  @Test
  public void testServerTimeout() throws Exception
  {
    final StreamRequest request =
        new StreamRequestBuilder(Bootstrap.createHttpURI(PORT, BUGGY_SERVER_URI)).build(EntityStreams.emptyStream());

    final CountDownLatch latch = new CountDownLatch(1);
    final AtomicInteger status = new AtomicInteger(-1);
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
        status.set(result.getStatus());
        result.getEntityStream().setReader(new Reader()
        {
          private ReadHandle _rh;

          @Override
          public void onInit(ReadHandle rh)
          {
            _rh = rh;
            _rh.request(Integer.MAX_VALUE);
          }

          @Override
          public void onDataAvailable(ByteString data)
          {
            // do nothing
          }

          @Override
          public void onDone()
          {
            // server would close the connection if TimeoutException, and netty would end the chunked transferring
            // with an empty chunk
            latch.countDown();
          }

          @Override
          public void onError(Throwable e)
          {
            latch.countDown();
          }
        });
      }
    });

    // server should timeout so await should return true
    Assert.assertTrue(latch.await(SERVER_IOHANDLER_TIMEOUT * 2, TimeUnit.MILLISECONDS));
    Assert.assertEquals(status.get(), RestStatus.OK);
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
  }

  private static class BuggyRequestHandler implements StreamRequestHandler
  {
    @Override
    public void handleRequest(StreamRequest request, RequestContext requestContext, Callback<StreamResponse> callback)
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
            _wh.write(ByteString.copy(new byte[50 * 1024]));
          }
        }

        @Override
        public void onAbort(Throwable e)
        {

        }
      };

      callback.onSuccess(new StreamResponseBuilder().build(EntityStreams.newEntityStream(noFinishWriter)));
    }
  }

}
