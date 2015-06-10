package test.r2.integ;

import com.linkedin.common.callback.Callback;
import com.linkedin.common.callback.FutureCallback;
import com.linkedin.common.util.None;
import com.linkedin.data.ByteString;
import com.linkedin.r2.message.RequestContext;
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
import com.linkedin.r2.transport.common.bridge.server.TransportDispatcher;
import com.linkedin.r2.transport.common.bridge.server.TransportDispatcherBuilder;
import com.linkedin.r2.transport.http.client.HttpClientFactory;
import junit.framework.Assert;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @author Zhenkai Zhu
 */
public class TestChannelPoolBehavior extends AbstractStreamTest
{
  private static final URI NOT_FOUND_URI = URI.create("/not_found");
  private static final URI NORMAL_URI = URI.create("/normal");
  private static final long WRITER_DELAY = 100;

  @Override
  protected TransportDispatcher getTransportDispatcher()
  {
    _scheduler = Executors.newSingleThreadScheduledExecutor();
    return new TransportDispatcherBuilder()
        .addStreamHandler(NOT_FOUND_URI, new NotFoundServerHandler())
        .addStreamHandler(NORMAL_URI, new NormalServerHandler())
        .build();
  }

  @Test
  public void testChannelBlocked() throws Exception
  {
    Client client = new TransportClientAdapter(_clientFactory.getClient(getClientProperties()));
    client.streamRequest(new StreamRequestBuilder(Bootstrap.createHttpURI(PORT, NOT_FOUND_URI))
        .build(EntityStreams.newEntityStream(new SlowWriter())), new Callback<StreamResponse>()
    {
      @Override
      public void onError(Throwable e)
      {
        throw new RuntimeException(e);
      }

      @Override
      public void onSuccess(StreamResponse result)
      {
        result.getEntityStream().setReader(new DrainReader());
      }
    });

    Future<RestResponse> responseFuture = _client.restRequest(new RestRequestBuilder(Bootstrap.createHttpURI(PORT, NORMAL_URI)).build());
    try
    {
      responseFuture.get(WRITER_DELAY/2 , TimeUnit.MILLISECONDS);
      Assert.fail();
    }
    catch (TimeoutException ex)
    {
      // expected
    }

    final FutureCallback<None> clientShutdownCallback = new FutureCallback<None>();
    client.shutdown(clientShutdownCallback);
    clientShutdownCallback.get();
  }

  @Test
  public void testChannelReuse() throws Exception
  {
    Client client = new TransportClientAdapter(_clientFactory.getClient(getClientProperties()));
    client.streamRequest(new StreamRequestBuilder(Bootstrap.createHttpURI(PORT, NOT_FOUND_URI))
        .build(EntityStreams.newEntityStream(new SlowWriter())), new Callback<StreamResponse>()
    {
      @Override
      public void onError(Throwable e)
      {
        throw new RuntimeException(e);
      }

      @Override
      public void onSuccess(StreamResponse result)
      {
        result.getEntityStream().setReader(new DrainReader());
      }
    });

    Future<RestResponse> responseFuture = _client.restRequest(new RestRequestBuilder(Bootstrap.createHttpURI(PORT, NORMAL_URI)).build());
    RestResponse response = responseFuture.get(WRITER_DELAY * 2 , TimeUnit.MILLISECONDS);
    Assert.assertEquals(response.getStatus(), RestStatus.OK);
    final FutureCallback<None> clientShutdownCallback = new FutureCallback<None>();
    client.shutdown(clientShutdownCallback);
    clientShutdownCallback.get();
  }

  @Override
  protected Map<String, String> getClientProperties()
  {
    Map<String, String> clientProperties = new HashMap<String, String>();
    clientProperties.put(HttpClientFactory.HTTP_POOL_SIZE, "2");
    clientProperties.put(HttpClientFactory.HTTP_POOL_MIN_SIZE, "1");
    return clientProperties;
  }

  private class NormalServerHandler implements StreamRequestHandler
  {
    @Override
    public void handleRequest(StreamRequest request, RequestContext requestContext, Callback<StreamResponse> callback)
    {
      callback.onSuccess(new StreamResponseBuilder().setStatus(RestStatus.OK).build(EntityStreams.emptyStream()));
      request.getEntityStream().setReader(new DrainReader());
    }
  }

  private class NotFoundServerHandler implements StreamRequestHandler
  {
    @Override
    public void handleRequest(StreamRequest request, RequestContext requestContext, Callback<StreamResponse> callback)
    {
      callback.onSuccess(new StreamResponseBuilder().setStatus(RestStatus.NOT_FOUND).build(EntityStreams.emptyStream()));
      request.getEntityStream().setReader(new Reader()
      {
        @Override
        public void onInit(ReadHandle rh)
        {
          rh.cancel();
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
  }

  private class SlowWriter implements Writer
  {

    private WriteHandle _wh;
    @Override
    public void onInit(final WriteHandle wh)
    {
      _wh = wh;
    }

    @Override
    public void onWritePossible()
    {
      _scheduler.schedule(new Runnable()
      {
        @Override
        public void run()
        {
          _wh.done();
        }
      }, WRITER_DELAY, TimeUnit.MILLISECONDS);
    }

    @Override
    public void onAbort(Throwable e)
    {

    }
  }
}
