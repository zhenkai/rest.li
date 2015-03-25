package test.r2.integ;

import com.linkedin.common.callback.Callback;
import com.linkedin.common.callback.FutureCallback;
import com.linkedin.common.util.None;
import com.linkedin.r2.message.RequestContext;
import com.linkedin.r2.message.rest.RestStatus;
import com.linkedin.r2.message.rest.StreamRequest;
import com.linkedin.r2.message.rest.StreamRequestBuilder;
import com.linkedin.r2.message.rest.StreamResponse;
import com.linkedin.r2.message.rest.StreamResponseBuilder;
import com.linkedin.r2.message.streaming.EntityStreams;
import com.linkedin.r2.message.streaming.ReadHandle;
import com.linkedin.r2.sample.Bootstrap;
import com.linkedin.r2.transport.common.Client;
import com.linkedin.r2.transport.common.StreamRequestHandler;
import com.linkedin.r2.transport.common.bridge.client.TransportClientAdapter;
import com.linkedin.r2.transport.common.bridge.server.StreamDispatcher;
import com.linkedin.r2.transport.common.bridge.server.StreamDispatcherBuilder;
import com.linkedin.r2.transport.http.client.HttpClientFactory;
import com.linkedin.r2.transport.http.server.HttpServer;
import com.linkedin.r2.transport.http.server.HttpServerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Zhenkai Zhu
 */
public class TestStreamEcho
{
  private HttpClientFactory _clientFactory;
  private static final int PORT = 8088;
  private static final URI ECHO_URI = URI.create("/echo");
  private static final long LARGE_BYTES_NUM = 1024 * 1024 * 1024;
  private static final long SMALL_BYTES_NUM = 1024 * 1024 * 32;
  private static final byte BYTE = 100;
  private static final long INTERVAL = 20;
  private HttpServer _server;
  private Client _client;
  private ScheduledExecutorService _scheduler;

  @BeforeSuite
  public void setup() throws IOException
  {
    _scheduler = Executors.newSingleThreadScheduledExecutor();
    _clientFactory = new HttpClientFactory();
    Map<String, String> clientProperties = new HashMap<String, String>();
    clientProperties.put(HttpClientFactory.HTTP_MAX_RESPONSE_SIZE, String.valueOf(LARGE_BYTES_NUM * 2));
    clientProperties.put(HttpClientFactory.HTTP_REQUEST_TIMEOUT, "500");
    _client = new TransportClientAdapter(_clientFactory.getClient(clientProperties));

    final StreamDispatcher dispatcher = new StreamDispatcherBuilder()
        .addStreamHandler(ECHO_URI, new SteamEchoHandler())
        .build();
    _server = new HttpServerFactory().createStreamServer(PORT, dispatcher);
    _server.start();
  }

  @Test
  public void testNormalEchoSmall() throws Exception
  {
    testNormalEcho(SMALL_BYTES_NUM);
  }

  @Test
  public void testNormalEchoLarge() throws Exception
  {
    testNormalEcho(LARGE_BYTES_NUM);
  }

  private void testNormalEcho(long bytesNum) throws Exception
  {
    BytesWriter writer = new BytesWriter(bytesNum, BYTE);
    StreamRequest request = new StreamRequestBuilder(Bootstrap.createHttpURI(PORT, ECHO_URI))
        .build(EntityStreams.newEntityStream(writer));

    final AtomicInteger status = new AtomicInteger(-1);
    final CountDownLatch latch = new CountDownLatch(1);
    final AtomicReference<Throwable> error = new AtomicReference<Throwable>();

    final Callback<None> readerCallback = new Callback<None>()
    {
      @Override
      public void onError(Throwable e)
      {
        error.set(e);
        latch.countDown();
      }

      @Override
      public void onSuccess(None result)
      {
        latch.countDown();
      }
    };

    final BytesReader reader = new BytesReader(BYTE, readerCallback);

    Callback<StreamResponse> callback = new Callback<StreamResponse>()
    {
      @Override
      public void onError(Throwable e)
      {
        readerCallback.onError(e);
      }

      @Override
      public void onSuccess(StreamResponse result)
      {
        status.set(result.getStatus());
        result.getEntityStream().setReader(reader);
      }
    };

    _client.streamRequest(request, callback);
    latch.await(60000, TimeUnit.MILLISECONDS);
    Assert.assertNull(error.get());
    Assert.assertEquals(status.get(), RestStatus.OK);
    Assert.assertEquals(reader.getTotalBytes(), bytesNum);
    Assert.assertTrue(reader.allBytesCorrect());
  }

  @Test
  public void testBackPressureEcho() throws Exception
  {
    TimedBytesWriter writer = new TimedBytesWriter(SMALL_BYTES_NUM, BYTE);
    StreamRequest request = new StreamRequestBuilder(Bootstrap.createHttpURI(PORT, ECHO_URI))
        .build(EntityStreams.newEntityStream(writer));

    final AtomicInteger status = new AtomicInteger(-1);
    final CountDownLatch latch = new CountDownLatch(1);
    final AtomicReference<Throwable> error = new AtomicReference<Throwable>();

    final Callback<None> readerCallback = new Callback<None>()
    {
      @Override
      public void onError(Throwable e)
      {
        error.set(e);
        latch.countDown();
      }

      @Override
      public void onSuccess(None result)
      {
        latch.countDown();
      }
    };

    final TimedBytesReader reader = new TimedBytesReader(BYTE, readerCallback)
    {
      int count = 0;

      @Override
      protected void requestMore(final ReadHandle rh, final int processedDataLen)
      {
        count ++;
        if (count % 16 == 0)
        {
          _scheduler.schedule(new Runnable()
          {
            @Override
            public void run()
            {
              rh.read(processedDataLen);
            }
          }, INTERVAL, TimeUnit.MILLISECONDS);
        }
        else
        {
          rh.read(processedDataLen);
        }
      }
    };

    Callback<StreamResponse> callback = new Callback<StreamResponse>()
    {
      @Override
      public void onError(Throwable e)
      {
        readerCallback.onError(e);
      }

      @Override
      public void onSuccess(StreamResponse result)
      {
        status.set(result.getStatus());
        result.getEntityStream().setReader(reader);
      }
    };

    _client.streamRequest(request, callback);
    latch.await(60000, TimeUnit.MILLISECONDS);
    Assert.assertNull(error.get());
    Assert.assertEquals(status.get(), RestStatus.OK);
    Assert.assertEquals(reader.getTotalBytes(), SMALL_BYTES_NUM);
    Assert.assertTrue(reader.allBytesCorrect());

    long clientSendTimespan = writer.getStopTime()- writer.getStartTime();
    long clientReceiveTimespan = reader.getStopTime() - reader.getStartTime();
    double diff = Math.abs(clientReceiveTimespan - clientSendTimespan);
    double diffRatio = diff / clientSendTimespan;
    Assert.assertTrue(diffRatio < 0.05);
  }

  @AfterSuite
  public void tearDown() throws Exception
  {
    _scheduler.shutdown();
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

  private static class SteamEchoHandler implements StreamRequestHandler
  {
    @Override
    public void handleRequest(StreamRequest request, RequestContext requestContext, Callback<StreamResponse> callback)
    {
      StreamResponseBuilder builder = new StreamResponseBuilder();
      callback.onSuccess(builder.build(request.getEntityStream()));
    }
  }
}
