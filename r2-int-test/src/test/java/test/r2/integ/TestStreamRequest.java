package test.r2.integ;

import com.linkedin.common.callback.Callback;
import com.linkedin.common.callback.FutureCallback;
import com.linkedin.common.util.None;
import com.linkedin.data.ByteString;
import com.linkedin.r2.message.RequestContext;
import com.linkedin.r2.message.rest.RestException;
import com.linkedin.r2.message.rest.RestResponse;
import com.linkedin.r2.message.rest.RestStatus;
import com.linkedin.r2.message.rest.StreamRequest;
import com.linkedin.r2.message.rest.StreamRequestBuilder;
import com.linkedin.r2.message.rest.StreamResponse;
import com.linkedin.r2.message.streaming.EntityStream;
import com.linkedin.r2.message.streaming.EntityStreams;

import com.linkedin.r2.message.streaming.FullEntityReader;
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
  private static final URI SERVICE_URI = URI.create("/upload");
  private static final int STATUS_CREATED = 202;
  private HttpServer _server;
  private ReceiveRequestHandler _requestHandler;

  @BeforeTest
  public void setup() throws IOException
  {
    _requestHandler = new ReceiveRequestHandler();
    _clientFactory = new HttpClientFactory();
    final StreamDispatcher dispatcher = new StreamDispatcherBuilder().addStreamHandler(SERVICE_URI, _requestHandler)
        .build();
    _server = new HttpServerFactory().createServer(PORT, dispatcher);
    _server.start();
  }

  @Test
  public void testRequest() throws Exception
  {
    Client client = new TransportClientAdapter(_clientFactory.getClient(Collections.<String, Object>emptyMap()));
    final int totalBytes = 1024 * 1024 * 10;
    EntityStream entityStream = EntityStreams.newEntityStream(new BytesWriter(totalBytes, (byte) 100));
    StreamRequestBuilder builder = new StreamRequestBuilder(Bootstrap.createHttpURI(PORT, SERVICE_URI));
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
    ByteString requestEntity = _requestHandler.getRequestEntity();
    Assert.assertEquals(requestEntity.length(), totalBytes);
    byte[] expected = new byte[totalBytes];
    Arrays.fill(expected, (byte) 100);
    Assert.assertEquals(expected, requestEntity.copyBytes());
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

  private static class ReceiveRequestHandler implements StreamRequestHandler
  {
    private ByteString _requestEntity;

    public void handleRequest(StreamRequest request, RequestContext requestContext, final Callback<StreamResponse> callback)
    {
      Callback<ByteString> entityCallback = new Callback<ByteString>()
      {
        @Override
        public void onError(Throwable e)
        {
          RestException restException = new RestException(RestStatus.responseForError(500, e));
          callback.onError(restException);
        }

        @Override
        public void onSuccess(ByteString result)
        {
          _requestEntity = result;
          RestResponse response = RestStatus.responseForStatus(STATUS_CREATED, "");
          callback.onSuccess(response);
        }
      };
      request.getEntityStream().setReader(new FullEntityReader(entityCallback));
    }

    public ByteString getRequestEntity()
    {
      return _requestEntity;
    }
  }
}
