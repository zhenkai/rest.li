package test.r2.integ;

import com.linkedin.common.callback.Callback;
import com.linkedin.common.callback.FutureCallback;
import com.linkedin.common.util.None;
import com.linkedin.data.ByteString;
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

import com.linkedin.r2.message.streaming.FullEntityReader;
import com.linkedin.r2.message.streaming.ReadHandle;
import com.linkedin.r2.message.streaming.Reader;
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
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
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
public class TestStreamRequest
{
  private HttpClientFactory _clientFactory;
  private static final int PORT = 8088;
  private static final URI LARGE_URI = URI.create("/large");
  private static final URI RATE_LIMITED_URI = URI.create("/rated-limited");
  private static final int STATUS_CREATED = 202;
  private static final byte BYTE = 100;
  private static final long INTERVAL = 20;
  private HttpServer _server;
  private ScheduledExecutorService _scheduler;
  private CheckRequestHandler _checkRequestHandler;
  private RateLimitedRequestHandler _rateLimitedRequestHandler;

  @BeforeSuite
  public void setup() throws IOException
  {
    _scheduler = Executors.newSingleThreadScheduledExecutor();
    _checkRequestHandler = new CheckRequestHandler(BYTE);
    _rateLimitedRequestHandler = new RateLimitedRequestHandler(_scheduler, INTERVAL, BYTE);
    _clientFactory = new HttpClientFactory();
    final StreamDispatcher dispatcher = new StreamDispatcherBuilder()
        .addStreamHandler(LARGE_URI, _checkRequestHandler)
        .addStreamHandler(RATE_LIMITED_URI, _rateLimitedRequestHandler)
        .build();
    _server = new HttpServerFactory().createServer(PORT, dispatcher);
    _server.start();
  }

  @Test
  public void testRequestLarge() throws Exception
  {
    Client client = new TransportClientAdapter(_clientFactory.getClient(Collections.<String, Object>emptyMap()));
    final int totalBytes = 1024 * 1024 * 1024;
    EntityStream entityStream = EntityStreams.newEntityStream(new BytesWriter(totalBytes, BYTE));
    StreamRequestBuilder builder = new StreamRequestBuilder(Bootstrap.createHttpURI(PORT, LARGE_URI));
    StreamRequest request = builder.setMethod("POST").build(entityStream);
    final AtomicInteger status = new AtomicInteger(-1);
    final CountDownLatch latch = new CountDownLatch(1);
    Callback<StreamResponse> callback = new Callback<StreamResponse>()
    {
      @Override
      public void onError(Throwable e)
      {
        latch.countDown();
        throw new RuntimeException(e);
      }

      @Override
      public void onSuccess(StreamResponse result)
      {
        status.set(result.getStatus());
        latch.countDown();
      }
    };
    client.streamRequest(request, callback);
    latch.await(60000, TimeUnit.MILLISECONDS);
    Assert.assertEquals(status.get(), STATUS_CREATED);
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
    Client client = new TransportClientAdapter(_clientFactory.getClient(Collections.<String, Object>emptyMap()));
    final int totalBytes = 1024 * 1024;
    EntityStream entityStream = EntityStreams.newEntityStream(new BytesWriter(totalBytes, BYTE));
    StreamRequestBuilder builder = new StreamRequestBuilder(Bootstrap.createHttpURI(PORT, URI.create("/boo")));
    StreamRequest request = builder.setMethod("POST").build(entityStream);
    final AtomicInteger status = new AtomicInteger(-1);
    final CountDownLatch latch = new CountDownLatch(1);
    Callback<StreamResponse> callback = new Callback<StreamResponse>()
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
    client.streamRequest(request, callback);
    latch.await(60000, TimeUnit.MILLISECONDS);
    Assert.assertEquals(status.get(), 404);
  }

  @Test
  public void testBackPressure() throws Exception
  {
    Client client = new TransportClientAdapter(_clientFactory.getClient(Collections.<String, Object>emptyMap()));
    final int totalBytes = 1024 * 1024 * 16;
    TimedBytesWriter writer = new TimedBytesWriter(totalBytes, BYTE);
    EntityStream entityStream = EntityStreams.newEntityStream(writer);
    StreamRequestBuilder builder = new StreamRequestBuilder(Bootstrap.createHttpURI(PORT, RATE_LIMITED_URI));
    StreamRequest request = builder.setMethod("POST").build(entityStream);
    final AtomicInteger status = new AtomicInteger(-1);
    final CountDownLatch latch = new CountDownLatch(1);
    Callback<StreamResponse> callback = new Callback<StreamResponse>()
    {
      @Override
      public void onError(Throwable e)
      {
        latch.countDown();
        throw new RuntimeException(e);
      }

      @Override
      public void onSuccess(StreamResponse result)
      {
        status.set(result.getStatus());
        latch.countDown();
      }
    };
    client.streamRequest(request, callback);
    latch.await(60000, TimeUnit.MILLISECONDS);
    Assert.assertEquals(status.get(), STATUS_CREATED);
    TimedBytesReader reader = _rateLimitedRequestHandler.getReader();
    Assert.assertNotNull(reader);
    Assert.assertEquals(totalBytes, reader.getTotalBytes());
    Assert.assertTrue(reader.allBytesCorrect());
    long clientSendTimespan = writer.getStopTime() - writer.getStartTime();
    long serverReceiveTimespan = reader.getStopTime() - reader.getStartTime();
    double diff = Math.abs(serverReceiveTimespan - clientSendTimespan);
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

    final FutureCallback<None> callback = new FutureCallback<None>();
    _clientFactory.shutdown(callback);
    callback.get();
  }

  private static class CheckRequestHandler implements StreamRequestHandler
  {
    private final byte _b;
    private BytesReader _reader;

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
          RestResponse response = RestStatus.responseForStatus(STATUS_CREATED, "");
          callback.onSuccess(response);
        }
      };
      _reader = new BytesReader(_b, readerCallback);
      request.getEntityStream().setReader(_reader);
    }

    BytesReader getReader()
    {
      return _reader;
    }
  }

  private static class RateLimitedRequestHandler implements StreamRequestHandler
  {
    private final ScheduledExecutorService _scheduler;
    private final long _interval;
    private TimedBytesReader _reader;
    private final byte _b;


    RateLimitedRequestHandler(ScheduledExecutorService scheduler, long interval, byte b)
    {
      _scheduler = scheduler;
      _interval = interval;
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
          RestResponse response = RestStatus.responseForStatus(STATUS_CREATED, "");
          callback.onSuccess(response);
        }
      };
      _reader = new TimedBytesReader(_b, readerCallback)
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
            }, _interval, TimeUnit.MILLISECONDS);
          }
          else
          {
            rh.read(processedDataLen);
          }
        }
      };

      request.getEntityStream().setReader(_reader);
    }

    TimedBytesReader getReader()
    {
      return _reader;
    }
  }
}
