package test.r2.integ;

import com.linkedin.common.callback.Callback;
import com.linkedin.common.callback.FutureCallback;
import com.linkedin.common.util.None;
import com.linkedin.r2.message.RequestContext;
import com.linkedin.r2.message.rest.RestRequestBuilder;
import com.linkedin.r2.message.rest.StreamRequest;
import com.linkedin.r2.message.rest.StreamResponse;
import com.linkedin.r2.message.rest.StreamResponseBuilder;
import com.linkedin.r2.message.streaming.EntityStreams;
import com.linkedin.r2.message.streaming.ReadHandle;
import com.linkedin.r2.message.streaming.WriteHandle;
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
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Zhenkai Zhu
 */
public class TestStreamResponse
{
  private HttpClientFactory _clientFactory;
  private static final int PORT = 8088;
  private static final URI LARGE_URI = URI.create("/large");
  private static final URI SMALL_URI = URI.create("/small");
  private static final URI SERVER_ERROR_URI = URI.create("/error");
  private static final long LARGE_BYTES_NUM = 1024 * 1024 * 1024;
  private static final long SMALL_BYTES_NUM = 1024 * 1024 * 16;
  private static final int STATUS_OK = 202;
  private static final byte BYTE = 100;
  private static final long INTERVAL = 20;
  private BytesWriterRequestHandler _largeHandler;
  private BytesWriterRequestHandler _smallHandler;
  private ErrorRequestHandler _errorHandler;
  private HttpServer _server;
  private ScheduledExecutorService _scheduler;
  private Map<String, String> _clientProperties;

  @BeforeSuite
  public void setup() throws IOException
  {
    _scheduler = Executors.newSingleThreadScheduledExecutor();
    _clientFactory = new HttpClientFactory();
    _largeHandler = new BytesWriterRequestHandler(BYTE, LARGE_BYTES_NUM);
    _smallHandler = new BytesWriterRequestHandler(BYTE, SMALL_BYTES_NUM);
    _errorHandler = new ErrorRequestHandler(SMALL_BYTES_NUM, BYTE);
    _clientProperties = new HashMap<String, String>();
    _clientProperties.put(HttpClientFactory.HTTP_MAX_RESPONSE_SIZE, String.valueOf(LARGE_BYTES_NUM * 2));
    final StreamDispatcher dispatcher = new StreamDispatcherBuilder()
        .addStreamHandler(LARGE_URI, _largeHandler)
        .addStreamHandler(SMALL_URI, _smallHandler)
        .addStreamHandler(SERVER_ERROR_URI, _errorHandler)
        .build();
    _server = new HttpServerFactory().createServer(PORT, dispatcher);
    _server.start();
  }

  @Test
  public void testRequestLarge() throws Exception
  {
    Client client = new TransportClientAdapter(_clientFactory.getClient(_clientProperties));
    RestRequestBuilder builder = new RestRequestBuilder(Bootstrap.createHttpURI(PORT, LARGE_URI));
    StreamRequest request = builder.build();
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

    client.streamRequest(request, callback);
    latch.await(60000, TimeUnit.MILLISECONDS);
    Assert.assertNull(error.get());
    Assert.assertEquals(status.get(), STATUS_OK);
    Assert.assertEquals(reader.getTotalBytes(), LARGE_BYTES_NUM);
    Assert.assertTrue(reader.allBytesCorrect());
  }

  @Test
  public void testErrorWhileStreaming() throws Exception
  {
    Map<String, String> clientProperties = new HashMap<String, String>(_clientProperties);
    clientProperties.put(HttpClientFactory.HTTP_REQUEST_TIMEOUT, "500");
    Client client = new TransportClientAdapter(_clientFactory.getClient(clientProperties));
    RestRequestBuilder builder = new RestRequestBuilder(Bootstrap.createHttpURI(PORT, SERVER_ERROR_URI));
    StreamRequest request = builder.build();
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

    client.streamRequest(request, callback);
    latch.await(60000, TimeUnit.MILLISECONDS);
    Assert.assertEquals(status.get(), STATUS_OK);
    Assert.assertNotNull(error.get());
    Throwable e = error.get();
    Assert.assertTrue(e instanceof TimeoutException);
    Assert.assertEquals(e.getMessage(), "Not receiving any chunk after timeout of 500ms");
  }

  @Test
  public void testBackpressure() throws Exception
  {
    Client client = new TransportClientAdapter(_clientFactory.getClient(_clientProperties));
    RestRequestBuilder builder = new RestRequestBuilder(Bootstrap.createHttpURI(PORT, SMALL_URI));
    StreamRequest request = builder.build();
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

    client.streamRequest(request, callback);
    latch.await(60000, TimeUnit.MILLISECONDS);
    Assert.assertNull(error.get());
    Assert.assertEquals(status.get(), STATUS_OK);
    long serverSendTimespan = _smallHandler.getWriter().getStopTime() - _smallHandler.getWriter().getStartTime();
    long clientReceiveTimespan = reader.getStopTime() - reader.getStartTime();
    double diff = Math.abs(clientReceiveTimespan - serverSendTimespan);
    double diffRatio = diff / serverSendTimespan;
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

    final FutureCallback<None> callback = new FutureCallback<None>();
    _clientFactory.shutdown(callback);
    callback.get();
  }

  private static class BytesWriterRequestHandler implements StreamRequestHandler
  {
    private final byte _b;
    private final long _bytesNum;
    private TimedBytesWriter _writer;

    BytesWriterRequestHandler(byte b, long bytesNUm)
    {
      _b = b;
      _bytesNum = bytesNUm;
    }

    @Override
    public void handleRequest(StreamRequest request, RequestContext requestContext, final Callback<StreamResponse> callback)
    {
      _writer = new TimedBytesWriter(_bytesNum, _b);
      StreamResponse response = new StreamResponseBuilder()
          .setStatus(STATUS_OK).build(EntityStreams.newEntityStream(_writer));
      callback.onSuccess(response);
    }

    TimedBytesWriter getWriter()
    {
      return _writer;
    }
  }

  private static class ErrorWriter extends BytesWriter
  {
    private final long _total;

    ErrorWriter(long total, byte fill)
    {
      super(total * 2, fill);
      _total = total;
    }

    @Override
    protected void afterWrite(WriteHandle wh, long written)
    {
      if (written > _total)
      {
        throw new RuntimeException();
      }
    }
  }

  private static class ErrorRequestHandler implements StreamRequestHandler
  {
    private final long _bytesNum;
    private final byte _b;

    ErrorRequestHandler(long bytesNum, byte b)
    {
      _bytesNum = bytesNum;
      _b = b;
    }
    @Override
    public void handleRequest(StreamRequest request, RequestContext requestContext, final Callback<StreamResponse> callback)
    {

      StreamResponse response = new StreamResponseBuilder()
          .setStatus(STATUS_OK).build(EntityStreams.newEntityStream(new ErrorWriter(_bytesNum, _b)));
      callback.onSuccess(response);
    }
  }
}
