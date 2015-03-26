package test.r2.integ;

import com.linkedin.common.callback.Callback;
import com.linkedin.common.callback.FutureCallback;
import com.linkedin.common.util.None;
import com.linkedin.r2.message.RequestContext;
import com.linkedin.r2.message.rest.RestRequestBuilder;
import com.linkedin.r2.message.rest.RestStatus;
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
import java.util.Random;
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
public class TestStreamResponse extends AbstractStreamTest
{
  private static final URI LARGE_URI = URI.create("/large");
  private static final URI SMALL_URI = URI.create("/small");
  private static final URI SERVER_ERROR_URI = URI.create("/error");
  private static final URI HICCUP_URI = URI.create("/hiccup");
  private BytesWriterRequestHandler _smallHandler;


  @Override
  protected StreamDispatcher getStreamDispatcher()
  {
    _smallHandler = new BytesWriterRequestHandler(BYTE, SMALL_BYTES_NUM);

    return new StreamDispatcherBuilder()
        .addStreamHandler(LARGE_URI, new BytesWriterRequestHandler(BYTE, LARGE_BYTES_NUM))
        .addStreamHandler(SMALL_URI, _smallHandler)
        .addStreamHandler(SERVER_ERROR_URI, new ErrorRequestHandler(BYTE, TINY_BYTES_NUM))
        .addStreamHandler(HICCUP_URI, new HiccupRequestHandler(BYTE, LARGE_BYTES_NUM, _scheduler))
        .build();
  }

  @Override
  protected Map<String, String> getClientProperties()
  {
    Map<String, String> clientProperties = new HashMap<String, String>();
    clientProperties.put(HttpClientFactory.HTTP_MAX_RESPONSE_SIZE, String.valueOf(LARGE_BYTES_NUM * 2));
    clientProperties.put(HttpClientFactory.HTTP_REQUEST_TIMEOUT, "500");
    return clientProperties;
  }

  @Test
  public void testResponseLarge() throws Exception
  {
    testResponse(Bootstrap.createHttpURI(PORT, LARGE_URI));
  }

  @Test
  public void testResponseHiccup() throws Exception
  {
    testResponse(Bootstrap.createHttpURI(PORT, HICCUP_URI));
  }

  private void testResponse(URI uri) throws Exception
  {
    RestRequestBuilder builder = new RestRequestBuilder(uri);
    StreamRequest request = builder.build();
    final AtomicInteger status = new AtomicInteger(-1);
    final CountDownLatch latch = new CountDownLatch(1);
    final AtomicReference<Throwable> error = new AtomicReference<Throwable>();

    final Callback<None> readerCallback = getReaderCallback(latch, error);
    final BytesReader reader = new BytesReader(BYTE, readerCallback);
    Callback<StreamResponse> callback = getCallback(status, readerCallback, reader);

    _client.streamRequest(request, callback);
    latch.await(60000, TimeUnit.MILLISECONDS);
    Assert.assertNull(error.get());
    Assert.assertEquals(status.get(), RestStatus.OK);
    Assert.assertEquals(reader.getTotalBytes(), LARGE_BYTES_NUM);
    Assert.assertTrue(reader.allBytesCorrect());
  }

  @Test
  public void testErrorWhileStreaming() throws Exception
  {
    RestRequestBuilder builder = new RestRequestBuilder(Bootstrap.createHttpURI(PORT, SERVER_ERROR_URI));
    StreamRequest request = builder.build();
    final AtomicInteger status = new AtomicInteger(-1);
    final CountDownLatch latch = new CountDownLatch(1);
    final AtomicReference<Throwable> error = new AtomicReference<Throwable>();

    final Callback<None> readerCallback = getReaderCallback(latch, error);
    final BytesReader reader = new BytesReader(BYTE, readerCallback);
    Callback<StreamResponse> callback = getCallback(status, readerCallback, reader);

    _client.streamRequest(request, callback);
    latch.await(60000, TimeUnit.MILLISECONDS);
    Assert.assertEquals(status.get(), RestStatus.OK);
    Throwable throwable = error.get();
    Assert.assertNotNull(throwable);
    Assert.assertTrue(throwable instanceof TimeoutException);
    Assert.assertEquals(throwable.getMessage(), "Not receiving any chunk after timeout of 500ms");
  }

  @Test
  public void testBackpressure() throws Exception
  {
    RestRequestBuilder builder = new RestRequestBuilder(Bootstrap.createHttpURI(PORT, SMALL_URI));
    StreamRequest request = builder.build();
    final AtomicInteger status = new AtomicInteger(-1);
    final CountDownLatch latch = new CountDownLatch(1);
    final AtomicReference<Throwable> error = new AtomicReference<Throwable>();

    final Callback<None> readerCallback = getReaderCallback(latch, error);
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
    Callback<StreamResponse> callback = getCallback(status, readerCallback, reader);

    _client.streamRequest(request, callback);
    latch.await(60000, TimeUnit.MILLISECONDS);
    Assert.assertNull(error.get());
    Assert.assertEquals(status.get(), RestStatus.OK);
    long serverSendTimespan = _smallHandler.getWriter().getStopTime() - _smallHandler.getWriter().getStartTime();
    long clientReceiveTimespan = reader.getStopTime() - reader.getStartTime();
    Assert.assertTrue(clientReceiveTimespan > 1000);
    double diff = Math.abs(clientReceiveTimespan - serverSendTimespan);
    double diffRatio = diff / serverSendTimespan;
    Assert.assertTrue(diffRatio < 0.05);
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
      _writer = createWriter(_bytesNum, _b);
      StreamResponse response = new StreamResponseBuilder().build(EntityStreams.newEntityStream(_writer));
      callback.onSuccess(response);
    }

    TimedBytesWriter getWriter()
    {
      return _writer;
    }

    protected TimedBytesWriter createWriter(long bytesNum, byte b)
    {
      return new TimedBytesWriter(_bytesNum, _b);
    }
  }

  private static class ErrorWriter extends TimedBytesWriter
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

  private static class ErrorRequestHandler extends BytesWriterRequestHandler
  {
    ErrorRequestHandler(byte b, long bytesNum)
    {
      super(b, bytesNum);
    }

    @Override
    protected TimedBytesWriter createWriter(long bytesNum, byte b)
    {
      return new ErrorWriter(bytesNum, b);
    }
  }

  private static class HiccupWriter extends TimedBytesWriter
  {
    private final Random _random = new Random();
    private final ScheduledExecutorService _scheduler;


    HiccupWriter(long total, byte fill, ScheduledExecutorService scheduler)
    {
      super(total, fill);
      _scheduler = scheduler;
    }

    @Override
    public void onWritePossible()
    {
      if (_random.nextInt() % 17 == 0)
      {
        _scheduler.schedule(new Runnable()
        {
          @Override
          public void run()
          {
            HiccupWriter.super.onWritePossible();
          }
        }, _random.nextInt() % 200, TimeUnit.MICROSECONDS);
      }
      else
      {
        super.onWritePossible();
      }
    }
  }

  private static class HiccupRequestHandler extends BytesWriterRequestHandler
  {
    private final ScheduledExecutorService _scheduler;

    HiccupRequestHandler(byte b, long bytesNum, ScheduledExecutorService scheduler)
    {
      super(b, bytesNum);
      _scheduler = scheduler;
    }

    @Override
    protected TimedBytesWriter createWriter(long bytesNum, byte b)
    {
      return new HiccupWriter(bytesNum, b, _scheduler);
    }
  }

  private Callback<None> getReaderCallback(final CountDownLatch latch, final AtomicReference<Throwable> error)
  {
    return new Callback<None>()
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
  }

  private Callback<StreamResponse> getCallback(final AtomicInteger status, final Callback<None> readerCallback, final BytesReader reader)
  {
    return new Callback<StreamResponse>()
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
  }

}
