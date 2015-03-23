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
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
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
  private static final int STATUS_CREATED = 202;
  private static final byte BYTE = 100;
  private HttpServer _server;
  private CheckRequestHandler _checkRequestHandler;

  @BeforeTest
  public void setup() throws IOException
  {
    _checkRequestHandler = new CheckRequestHandler(BYTE);
    _clientFactory = new HttpClientFactory();
    final StreamDispatcher dispatcher = new StreamDispatcherBuilder().addStreamHandler(LARGE_URI, _checkRequestHandler)
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
    Assert.assertEquals(totalBytes, _checkRequestHandler.getTotalBytes());
    Assert.assertTrue(_checkRequestHandler.allBytesCorrect());
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

  @AfterTest
  public void tearDown() throws Exception
  {

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
    private int _length;
    private boolean _bytesCorrect;

    CheckRequestHandler(byte b)
    {
      _b = b;
    }

    @Override
    public void handleRequest(StreamRequest request, RequestContext requestContext, final Callback<StreamResponse> callback)
    {
      _length = 0;
      _bytesCorrect = true;

      request.getEntityStream().setReader(new Reader()
      {
        private ReadHandle _rh;

        @Override
        public void onInit(ReadHandle rh)
        {
          _rh = rh;
          _rh.read(16 * 1024);
        }

        @Override
        public void onDataAvailable(ByteString data)
        {
          _length += data.length();
          byte [] bytes = data.copyBytes();
          for (byte b : bytes)
          {
            if (b != _b)
            {
              _bytesCorrect = false;
            }
          }
          _rh.read(data.length());
        }

        @Override
        public void onDone()
        {
          RestResponse response = RestStatus.responseForStatus(STATUS_CREATED, "");
          callback.onSuccess(response);
        }

        @Override
        public void onError(Throwable e)
        {
          RestException restException = new RestException(RestStatus.responseForError(500, e));
          callback.onError(restException);
        }
      });
    }

    public int getTotalBytes()
    {
      return _length;
    }

    public boolean allBytesCorrect()
    {
      return _bytesCorrect;
    }
  }
}
