package test.r2.integ;

import com.linkedin.common.callback.Callback;
import com.linkedin.common.util.None;
import com.linkedin.r2.message.RequestContext;
import com.linkedin.r2.message.rest.RestException;
import com.linkedin.r2.message.rest.RestResponse;
import com.linkedin.r2.message.rest.RestStatus;
import com.linkedin.r2.message.rest.StreamException;
import com.linkedin.r2.message.rest.StreamRequest;
import com.linkedin.r2.message.rest.StreamRequestBuilder;
import com.linkedin.r2.message.rest.StreamResponse;
import com.linkedin.r2.message.streaming.EntityStream;
import com.linkedin.r2.message.streaming.EntityStreams;

import com.linkedin.r2.message.streaming.ReadHandle;
import com.linkedin.r2.sample.Bootstrap;
import com.linkedin.r2.transport.common.StreamRequestHandler;
import com.linkedin.r2.transport.common.bridge.server.StreamDispatcher;
import com.linkedin.r2.transport.common.bridge.server.StreamDispatcherBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class tests client sending streaming request && server receiving streaming request
 *
 * @author Zhenkai Zhu
 */
public class TestStreamRequest extends AbstractStreamTest
{

  private static final URI LARGE_URI = URI.create("/large");
  private static final URI RATE_LIMITED_URI = URI.create("/rated-limited");
  private CheckRequestHandler _checkRequestHandler;
  private RateLimitedRequestHandler _rateLimitedRequestHandler;

  @Override
  protected StreamDispatcher getStreamDispatcher()
  {
    _scheduler = Executors.newSingleThreadScheduledExecutor();
    _checkRequestHandler = new CheckRequestHandler(BYTE);
    _rateLimitedRequestHandler = new RateLimitedRequestHandler(_scheduler, INTERVAL, BYTE);
    return new StreamDispatcherBuilder()
        .addStreamHandler(LARGE_URI, _checkRequestHandler)
        .addStreamHandler(RATE_LIMITED_URI, _rateLimitedRequestHandler)
        .build();
  }

  @Test
  public void testRequestLarge() throws Exception
  {
    final long totalBytes = LARGE_BYTES_NUM;
    EntityStream entityStream = EntityStreams.newEntityStream(new BytesWriter(totalBytes, BYTE));
    StreamRequestBuilder builder = new StreamRequestBuilder(Bootstrap.createHttpURI(PORT, LARGE_URI));
    StreamRequest request = builder.setMethod("POST").build(entityStream);



    final AtomicInteger status = new AtomicInteger(-1);
    final CountDownLatch latch = new CountDownLatch(1);
    Callback<StreamResponse> callback = expectSuccessCallback(latch, status);
    _client.streamRequest(request, callback);
    latch.await(60000, TimeUnit.MILLISECONDS);
    Assert.assertEquals(status.get(), RestStatus.OK);
    BytesReader reader = _checkRequestHandler.getReader();
    Assert.assertNotNull(reader);
    Assert.assertEquals(totalBytes, reader.getTotalBytes());
    Assert.assertTrue(reader.allBytesCorrect());
  }

  // TODO [ZZ]: is it R2's responsibility to stop the writer from writing more data when server side error happens?
  // or should the user do it in their callback?
  @Test
  public void test404() throws Exception
  {
    final long totalBytes = TINY_BYTES_NUM;
    EntityStream entityStream = EntityStreams.newEntityStream(new BytesWriter(totalBytes, BYTE));
    StreamRequestBuilder builder = new StreamRequestBuilder(Bootstrap.createHttpURI(PORT, URI.create("/boo")));
    StreamRequest request = builder.setMethod("POST").build(entityStream);
    final AtomicInteger status = new AtomicInteger(-1);
    final CountDownLatch latch = new CountDownLatch(1);
    Callback<StreamResponse> callback = expectErrorCallback(latch, status);
    _client.streamRequest(request, callback);
    latch.await(60000, TimeUnit.MILLISECONDS);
    Assert.assertEquals(status.get(), 404);
  }

  @Test
  public void testBackPressure() throws Exception
  {
    final long totalBytes = SMALL_BYTES_NUM;
    TimedBytesWriter writer = new TimedBytesWriter(totalBytes, BYTE);
    EntityStream entityStream = EntityStreams.newEntityStream(writer);
    StreamRequestBuilder builder = new StreamRequestBuilder(Bootstrap.createHttpURI(PORT, RATE_LIMITED_URI));
    StreamRequest request = builder.setMethod("POST").build(entityStream);
    final AtomicInteger status = new AtomicInteger(-1);
    final CountDownLatch latch = new CountDownLatch(1);
    Callback<StreamResponse> callback = expectSuccessCallback(latch, status);
    _client.streamRequest(request, callback);
    latch.await(60000, TimeUnit.MILLISECONDS);
    Assert.assertEquals(status.get(), RestStatus.OK);
    TimedBytesReader reader = _rateLimitedRequestHandler.getReader();
    Assert.assertNotNull(reader);
    Assert.assertEquals(totalBytes, reader.getTotalBytes());
    Assert.assertTrue(reader.allBytesCorrect());
    long clientSendTimespan = writer.getStopTime() - writer.getStartTime();
    long serverReceiveTimespan = reader.getStopTime() - reader.getStartTime();
    Assert.assertTrue(serverReceiveTimespan > 1000);
    double diff = Math.abs(serverReceiveTimespan - clientSendTimespan);
    double diffRatio = diff / clientSendTimespan;
    Assert.assertTrue(diffRatio < 0.05);
  }

  private static class CheckRequestHandler implements StreamRequestHandler
  {
    private final byte _b;
    private TimedBytesReader _reader;

    CheckRequestHandler(byte b)
    {
      _b = b;
    }

    @Override
    public void handleRequest(StreamRequest request, RequestContext requestContext, final Callback<StreamResponse> callback)
    {
      Callback<None> readerCallback = new Callback<None>()
      {
        @Override
        public void onError(Throwable e)
        {
          RestException restException = new RestException(RestStatus.responseForError(500, e));
          callback.onError(restException);
        }

        @Override
        public void onSuccess(None result)
        {
          RestResponse response = RestStatus.responseForStatus(RestStatus.OK, "");
          callback.onSuccess(response);
        }
      };
      _reader = createReader(_b, readerCallback);
      request.getEntityStream().setReader(_reader);
    }

    TimedBytesReader getReader()
    {
      return _reader;
    }

    protected TimedBytesReader createReader(byte b, Callback<None> readerCallback)
    {
      return new TimedBytesReader(_b, readerCallback);
    }
  }

  private static class RateLimitedRequestHandler extends CheckRequestHandler
  {
    private final ScheduledExecutorService _scheduler;
    private final long _interval;


    RateLimitedRequestHandler(ScheduledExecutorService scheduler, long interval, byte b)
    {
      super((b));
      _scheduler = scheduler;
      _interval = interval;
    }

    @Override
    protected TimedBytesReader createReader(byte b, Callback<None> readerCallback)
    {
      return new TimedBytesReader(b, readerCallback)
      {
        int count = 0;

        @Override
        public void requestMore(final ReadHandle rh, final int processedDataLen)
        {
          count++;
          if (count % 16 == 0)
          {
            _scheduler.schedule(new Runnable()
            {
              @Override
              public void run()
              {
                rh.read(processedDataLen);
              }
            },_interval, TimeUnit.MILLISECONDS);
          }
          else
          {
            rh.read(processedDataLen);
          }
        }
      };
    }
  }

  private static Callback<StreamResponse> expectErrorCallback(final CountDownLatch latch, final AtomicInteger status)
  {
    return new Callback<StreamResponse>()
    {
      @Override
      public void onError(Throwable e)
      {
        if (e instanceof StreamException)
        {
          StreamResponse errorResponse = ((StreamException) e).getResponse();
          status.set(errorResponse.getStatus());
        }
        latch.countDown();
      }

      @Override
      public void onSuccess(StreamResponse result)
      {
        latch.countDown();
        throw new RuntimeException("Should have failed with 404");
      }
    };
  }

  private static Callback<StreamResponse> expectSuccessCallback(final CountDownLatch latch, final AtomicInteger status)
  {
    return new Callback<StreamResponse>()
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
        latch.countDown();
      }
    };
  }
}
