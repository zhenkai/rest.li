/*
   Copyright (c) 2012 LinkedIn Corp.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

/**
 * $Id: $
 */

package com.linkedin.r2.transport.http.client;

import io.netty.channel.Channel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.handler.codec.TooLongFrameException;
import java.io.IOException;
import java.net.SocketAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;

import com.linkedin.data.ByteString;
import com.linkedin.r2.message.Messages;
import com.linkedin.r2.message.stream.StreamResponse;
import com.linkedin.r2.message.entitystream.ReadHandle;
import com.linkedin.r2.message.entitystream.Reader;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.common.callback.Callback;
import com.linkedin.common.callback.FutureCallback;
import com.linkedin.common.util.None;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.r2.message.RequestContext;
import com.linkedin.r2.message.rest.RestRequest;
import com.linkedin.r2.message.rest.RestRequestBuilder;
import com.linkedin.r2.transport.common.bridge.client.TransportCallbackAdapter;
import com.linkedin.r2.transport.common.bridge.common.TransportCallback;

/**
 * @author Steven Ihde
 * @author Ang Xu
 * @version $Revision: $
 */

public class TestHttpNettyClient
{
  private NioEventLoopGroup _eventLoop;
  private ScheduledExecutorService _scheduler;

  private static final int TEST_MAX_RESPONSE_SIZE = 500000;
  private static final int TEST_MAX_HEADER_SIZE = 50000;

  private static final int RESPONSE_OK = 1;
  private static final int TOO_LARGE = 2;

  @BeforeClass
  public void setup()
  {
    _eventLoop = new NioEventLoopGroup();
    _scheduler = Executors.newSingleThreadScheduledExecutor();
  }

  @AfterClass
  public void tearDown()
  {
    _scheduler.shutdown();
    _eventLoop.shutdownGracefully();
  }

  @Test
  public void testNoChannelTimeout()
      throws InterruptedException
  {
    HttpNettyStreamClient client = new HttpNettyStreamClient(new NoCreations(_scheduler), _scheduler, 500, 500, 1024 * 1024 * 2);

    RestRequest r = new RestRequestBuilder(URI.create("http://localhost/")).build();

    FutureCallback<StreamResponse> cb = new FutureCallback<StreamResponse>();
    TransportCallback<StreamResponse> callback = new TransportCallbackAdapter<StreamResponse>(cb);

    client.streamRequest(Messages.toStreamRequest(r), new RequestContext(), new HashMap<String, String>(), callback);

    try
    {
      // This timeout needs to be significantly larger than the getTimeout of the netty client;
      // we're testing that the client will generate its own timeout
      cb.get(30, TimeUnit.SECONDS);
      Assert.fail("Get was supposed to time out");
    }
    catch (TimeoutException e)
    {
      // TimeoutException means the timeout for Future.get() elapsed and nothing happened.
      // Instead, we are expecting our callback to be invoked before the future timeout
      // with a timeout generated by the HttpNettyClient.
      Assert.fail("Unexpected TimeoutException, should have been ExecutionException", e);
    }
    catch (ExecutionException e)
    {
      verifyCauseChain(e, RemoteInvocationException.class, TimeoutException.class);
    }
  }

  @Test
  public void testNoResponseTimeout()
      throws InterruptedException, IOException
  {
    TestServer testServer = new TestServer();

    HttpNettyStreamClient client = new HttpClientBuilder(_eventLoop, _scheduler).setRequestTimeout(500).setIdleTimeout(10000)
        .setShutdownTimeout(500).build();

    RestRequest r = new RestRequestBuilder(testServer.getNoResponseURI()).build();
    FutureCallback<StreamResponse> cb = new FutureCallback<StreamResponse>();
    TransportCallback<StreamResponse> callback = new TransportCallbackAdapter<StreamResponse>(cb);
    client.streamRequest(Messages.toStreamRequest(r), new RequestContext(), new HashMap<String, String>(), callback);

    try
    {
      // This timeout needs to be significantly larger than the getTimeout of the netty client;
      // we're testing that the client will generate its own timeout
      cb.get(30, TimeUnit.SECONDS);
      Assert.fail("Get was supposed to time out");
    }
    catch (TimeoutException e)
    {
      // TimeoutException means the timeout for Future.get() elapsed and nothing happened.
      // Instead, we are expecting our callback to be invoked before the future timeout
      // with a timeout generated by the HttpNettyClient.
      Assert.fail("Unexpected TimeoutException, should have been ExecutionException", e);
    }
    catch (ExecutionException e)
    {
      verifyCauseChain(e, RemoteInvocationException.class, TimeoutException.class);
    }
    testServer.shutdown();
  }

  @Test
  public void testBadAddress() throws InterruptedException, IOException, TimeoutException
  {
    HttpNettyStreamClient client = new HttpClientBuilder(_eventLoop, _scheduler)
                                  .setRequestTimeout(30000)
                                  .setIdleTimeout(10000)
                                  .setShutdownTimeout(500)
                                  .build();

    RestRequest r = new RestRequestBuilder(URI.create("http://this.host.does.not.exist.linkedin.com")).build();
    FutureCallback<StreamResponse> cb = new FutureCallback<StreamResponse>();
    TransportCallback<StreamResponse> callback = new TransportCallbackAdapter<StreamResponse>(cb);
    client.streamRequest(Messages.toStreamRequest(r), new RequestContext(), new HashMap<String,String>(), callback);
    try
    {
      cb.get(30, TimeUnit.SECONDS);
      Assert.fail("Get was supposed to fail");
    }
    catch (ExecutionException e)
    {
      verifyCauseChain(e, RemoteInvocationException.class, UnknownHostException.class);
    }
  }

  @Test
  public void testMaxResponseSizeOK() throws InterruptedException, IOException, TimeoutException
  {
    testResponseSize(TEST_MAX_RESPONSE_SIZE - 1, RESPONSE_OK);

    testResponseSize(TEST_MAX_RESPONSE_SIZE, RESPONSE_OK);
  }

  @Test
  public void setTestMaxResponseSizeTooLarge() throws InterruptedException, IOException, TimeoutException
  {
    testResponseSize(TEST_MAX_RESPONSE_SIZE+1, TOO_LARGE);
  }

  public void testResponseSize(int responseSize, int expectedResult)
      throws InterruptedException, IOException, TimeoutException
  {
    TestServer testServer = new TestServer();

    HttpNettyStreamClient client =
        new HttpClientBuilder(_eventLoop, _scheduler).setRequestTimeout(50000).setIdleTimeout(10000)
            .setShutdownTimeout(500).setMaxResponseSize(TEST_MAX_RESPONSE_SIZE).build();

    RestRequest r = new RestRequestBuilder(testServer.getResponseOfSizeURI(responseSize)).build();
    FutureCallback<StreamResponse> cb = new FutureCallback<StreamResponse>();
    TransportCallback<StreamResponse> callback = new TransportCallbackAdapter<StreamResponse>(cb);
    client.streamRequest(Messages.toStreamRequest(r), new RequestContext(), new HashMap<String, String>(), callback);

    try
    {
      StreamResponse response = cb.get(30, TimeUnit.SECONDS);
      final CountDownLatch latch = new CountDownLatch(1);
      final AtomicReference<Throwable> error = new AtomicReference<Throwable>();
      response.getEntityStream().setReader(new Reader()
      {
        @Override
        public void onInit(ReadHandle rh)
        {
          rh.request(Integer.MAX_VALUE);
        }

        @Override
        public void onDataAvailable(ByteString data)
        {
        }

        @Override
        public void onDone()
        {
          latch.countDown();
        }

        @Override
        public void onError(Throwable e)
        {
          error.set(e);
          latch.countDown();
        }
      });

      latch.await(30, TimeUnit.SECONDS);

      if(expectedResult == TOO_LARGE)
      {
        Assert.assertNotNull(error.get(), "Max response size exceeded, expected exception. ");
        verifyCauseChain(error.get(), TooLongFrameException.class);
      }
      if (expectedResult == RESPONSE_OK)
      {
        Assert.assertNull(error.get(), "Unexpected Exception: response size <= max size");
      }
    }
    catch (ExecutionException e)
    {
      if (expectedResult == RESPONSE_OK)
      {
        Assert.fail("Unexpected ExecutionException, response was <= max response size.");
      }
      verifyCauseChain(e, RemoteInvocationException.class, TooLongFrameException.class);
    }
    testServer.shutdown();
  }

  @Test
  public void testMaxHeaderSize() throws InterruptedException, IOException, TimeoutException
  {
    testHeaderSize(TEST_MAX_HEADER_SIZE - 1, RESPONSE_OK);

    testHeaderSize(TEST_MAX_HEADER_SIZE, RESPONSE_OK);

    testHeaderSize(TEST_MAX_HEADER_SIZE + 1, TOO_LARGE);
  }

  public void testHeaderSize(int headerSize, int expectedResult)
      throws InterruptedException, IOException, TimeoutException
  {
    TestServer testServer = new TestServer();

    HttpNettyStreamClient client =
        new HttpClientBuilder(_eventLoop, _scheduler).setRequestTimeout(5000000).setIdleTimeout(10000)
            .setShutdownTimeout(500).setMaxHeaderSize(TEST_MAX_HEADER_SIZE).build();

    RestRequest r = new RestRequestBuilder(testServer.getResponseWithHeaderSizeURI(headerSize)).build();
    FutureCallback<StreamResponse> cb = new FutureCallback<StreamResponse>();
    TransportCallback<StreamResponse> callback = new TransportCallbackAdapter<StreamResponse>(cb);
    client.streamRequest(Messages.toStreamRequest(r), new RequestContext(), new HashMap<String, String>(), callback);

    try
    {
      cb.get(300, TimeUnit.SECONDS);
      if (expectedResult == TOO_LARGE)
      {
        Assert.fail("Max header size exceeded, expected exception. ");
      }
    }
    catch (ExecutionException e)
    {
      if (expectedResult == RESPONSE_OK)
      {
        Assert.fail("Unexpected ExecutionException, header was <= max header size.");
      }
      verifyCauseChain(e, RemoteInvocationException.class, TooLongFrameException.class);
    }
    testServer.shutdown();
  }

  @Test
  public void testBadHeader() throws InterruptedException, IOException
  {
    TestServer testServer = new TestServer();
    HttpNettyStreamClient client = new HttpClientBuilder(_eventLoop, _scheduler)
                                  .setRequestTimeout(10000)
                                  .setIdleTimeout(10000)
                                  .setShutdownTimeout(500).build();

    RestRequest r = new RestRequestBuilder(testServer.getBadHeaderURI()).build();
    FutureCallback<StreamResponse> cb = new FutureCallback<StreamResponse>();
    TransportCallback<StreamResponse> callback = new TransportCallbackAdapter<StreamResponse>(cb);
    client.streamRequest(Messages.toStreamRequest(r), new RequestContext(), new HashMap<String, String>(), callback);

    try
    {
      cb.get(30, TimeUnit.SECONDS);
      Assert.fail("Get was supposed to fail");
    }
    catch (TimeoutException e)
    {
      Assert.fail("Unexpected TimeoutException, should have been ExecutionException", e);
    }
    catch (ExecutionException e)
    {
      verifyCauseChain(e, RemoteInvocationException.class, IllegalArgumentException.class);
    }
    testServer.shutdown();
  }

  @Test
  public void testShutdown() throws ExecutionException, TimeoutException, InterruptedException
  {
    HttpNettyStreamClient client = new HttpClientBuilder(_eventLoop, _scheduler)
                                  .setRequestTimeout(500)
                                  .setIdleTimeout(10000)
                                  .setShutdownTimeout(500)
                                  .build();

    FutureCallback<None> shutdownCallback = new FutureCallback<None>();
    client.shutdown(shutdownCallback);
    shutdownCallback.get(30, TimeUnit.SECONDS);

    // Now verify a new request will also fail
    RestRequest r = new RestRequestBuilder(URI.create("http://no.such.host.linkedin.com")).build();
    FutureCallback<StreamResponse> callback = new FutureCallback<StreamResponse>();
    client.streamRequest(Messages.toStreamRequest(r), new RequestContext(), new HashMap<String,String>(), new TransportCallbackAdapter<StreamResponse>(callback));
    try
    {
      callback.get(30, TimeUnit.SECONDS);
    }
    catch (ExecutionException e)
    {
      // Expected
    }
  }

  @Test
  public void testShutdownStuckInPool()
      throws InterruptedException, ExecutionException, TimeoutException

  {
    // Test that shutdown works when the outstanding request is stuck in the pool waiting for a channel
    HttpNettyStreamClient client = new HttpNettyStreamClient(new NoCreations(_scheduler), _scheduler, 60000, 1, 1024 * 1024 * 2);

    RestRequest r = new RestRequestBuilder(URI.create("http://some.host/")).build();
    FutureCallback<StreamResponse> futureCallback = new FutureCallback<StreamResponse>();
    client.streamRequest(Messages.toStreamRequest(r), new RequestContext(), new HashMap<String, String>(), new TransportCallbackAdapter<StreamResponse>(futureCallback));

    FutureCallback<None> shutdownCallback = new FutureCallback<None>();
    client.shutdown(shutdownCallback);

    shutdownCallback.get(30, TimeUnit.SECONDS);

    try
    {
      futureCallback.get(30, TimeUnit.SECONDS);
      Assert.fail("get should have thrown exception");
    }
    catch (ExecutionException e)
    {
      verifyCauseChain(e, RemoteInvocationException.class, TimeoutException.class);
    }
  }

  @Test
  public void testShutdownRequestOutstanding()
      throws IOException, ExecutionException, TimeoutException, InterruptedException
  {
    // Test that it works when the shutdown kills the outstanding request...
    testShutdownRequestOutstanding(500, 60000, RemoteInvocationException.class, TimeoutException.class);
  }

  @Test
  public void testShutdownRequestOutstanding2()
      throws IOException, ExecutionException, TimeoutException, InterruptedException
  {
    // Test that it works when the request timeout kills the outstanding request...
    testShutdownRequestOutstanding(60000, 500, RemoteInvocationException.class,
        // sometimes the test fails with ChannelClosedException
        // TimeoutException.class
        Exception.class);
  }

  private void testShutdownRequestOutstanding(int shutdownTimeout, int requestTimeout, Class<?>... causeChain)
      throws InterruptedException, IOException, ExecutionException, TimeoutException
  {
    TestServer testServer = new TestServer();

    HttpNettyStreamClient client = new HttpClientBuilder(_eventLoop, _scheduler).setRequestTimeout(requestTimeout)
        .setShutdownTimeout(shutdownTimeout).build();

    RestRequest r = new RestRequestBuilder(testServer.getNoResponseURI()).build();
    FutureCallback<StreamResponse> cb = new FutureCallback<StreamResponse>();
    TransportCallback<StreamResponse> callback = new TransportCallbackAdapter<StreamResponse>(cb);
    client.streamRequest(Messages.toStreamRequest(r), new RequestContext(), new HashMap<String,String>(), callback);

    FutureCallback<None> shutdownCallback = new FutureCallback<None>();
    client.shutdown(shutdownCallback);
    shutdownCallback.get(30, TimeUnit.SECONDS);

    try
    {
      // This timeout needs to be significantly larger than the getTimeout of the netty client;
      // we're testing that the client will generate its own timeout
      cb.get(30, TimeUnit.SECONDS);
      Assert.fail("Get was supposed to time out");
    }
    catch (TimeoutException e)
    {
      // TimeoutException means the timeout for Future.get() elapsed and nothing happened.
      // Instead, we are expecting our callback to be invoked before the future timeout
      // with a timeout generated by the HttpNettyClient.
      Assert.fail("Get timed out, should have thrown ExecutionException", e);
    }
    catch (ExecutionException e)
    {
      verifyCauseChain(e, causeChain);
    }
    testServer.shutdown();
  }

  private static void verifyCauseChain(Throwable throwable, Class<?>... causes)
  {
    Throwable t = throwable;
    for (Class<?> c : causes)
    {
      Throwable cause = t.getCause();
      if (cause == null)
      {
        Assert.fail("Cause chain ended too early", throwable);
      }
      if (!c.isAssignableFrom(cause.getClass()))
      {
        Assert.fail("Expected cause " + c.getName() + " not " + cause.getClass().getName(), throwable);
      }
      t = cause;
    }
  }

  // Test that cannot pass pass SSLParameters without SSLContext.
  // This in fact tests HttpClientPipelineFactory constructor through HttpNettyClient
  // constructor.
  @Test
  public void testClientPipelineFactory1()
      throws NoSuchAlgorithmException
  {
    try
    {
      new HttpClientBuilder(_eventLoop, _scheduler)
          .setSSLParameters(new SSLParameters())
          .build();
    }
    catch (IllegalArgumentException e)
    {
      // Check exception message to make sure it's the expected one.
      Assert.assertEquals(e.getMessage(), "SSLParameters passed with no SSLContext");
    }
  }

  // Test that cannot set cipher suites in SSLParameters that don't have any match in
  // SSLContext.
  // This in fact tests HttpClientPipelineFactory constructor through HttpNettyClient
  // constructor.
  @Test
  public void testClientPipelineFactory2Fail()
      throws NoSuchAlgorithmException
  {
    String[] requestedCipherSuites = {"Unsupported"};
    SSLParameters sslParameters = new SSLParameters();
    sslParameters.setCipherSuites(requestedCipherSuites);
    try
    {
      new HttpClientBuilder(_eventLoop, _scheduler)
          .setSSLContext(SSLContext.getDefault())
          .setSSLParameters(sslParameters)
          .build();
    }
    catch (IllegalArgumentException e)
    {
      // Check exception message to make sure it's the expected one.
      Assert.assertEquals(e.getMessage(), "None of the requested cipher suites: [Unsupported] are found in SSLContext");
    }
  }

  // Test that can set cipher suites in SSLParameters that have at least one match in
  // SSLContext.
  // This in fact tests HttpClientPipelineFactory constructor through HttpNettyClient
  // constructor.
  @Test
  public void testClientPipelineFactory2Pass()
      throws NoSuchAlgorithmException
  {
    String[] requestedCipherSuites = {"Unsupported", "SSL_RSA_WITH_RC4_128_SHA"};
    SSLParameters sslParameters = new SSLParameters();
    sslParameters.setCipherSuites(requestedCipherSuites);
    new HttpClientBuilder(_eventLoop, _scheduler)
        .setSSLContext(SSLContext.getDefault())
        .setSSLParameters(sslParameters)
        .build();
  }

  // Test that cannot set protocols in SSLParameters that don't have any match in
  // SSLContext.
  // This in fact tests HttpClientPipelineFactory constructor through HttpNettyClient
  // constructor.
  @Test
  public void testClientPipelineFactory3Fail()
      throws NoSuchAlgorithmException
  {
    String[] requestedProtocols = {"Unsupported"};
    SSLParameters sslParameters = new SSLParameters();
    sslParameters.setProtocols(requestedProtocols);
    try
    {
      new HttpClientBuilder(_eventLoop, _scheduler)
          .setSSLContext(SSLContext.getDefault())
          .setSSLParameters(sslParameters)
          .build();
    }
    catch (IllegalArgumentException e)
    {
      // Check exception message to make sure it's the expected one.
      Assert.assertEquals(e.getMessage(), "None of the requested protocols: [Unsupported] are found in SSLContext");
    }
  }

  // Test that can set protocols in SSLParameters that have at least one match in
  // SSLContext.
  // This in fact tests HttpClientPipelineFactory constructor through HttpNettyClient
  // constructor.
  @Test
  public void testClientPipelineFactory3Pass()
      throws NoSuchAlgorithmException
  {
    String[] requestedProtocols = {"Unsupported", "TLSv1"};
    SSLParameters sslParameters = new SSLParameters();
    sslParameters.setProtocols(requestedProtocols);

    new HttpClientBuilder(_eventLoop, _scheduler)
        .setSSLContext(SSLContext.getDefault())
        .setSSLParameters(sslParameters)
        .build();
  }

  @Test
  public void testPoolStatsProviderManager()
      throws InterruptedException, ExecutionException, TimeoutException
  {
    final CountDownLatch setLatch = new CountDownLatch(1);
    final CountDownLatch removeLatch = new CountDownLatch(1);
    AbstractJmxManager manager = new AbstractJmxManager()
    {
      @Override
      public void onProviderCreate(PoolStatsProvider provider)
      {
        setLatch.countDown();
      }

      @Override
      public void onProviderShutdown(PoolStatsProvider provider)
      {
        removeLatch.countDown();
      }
    };

    HttpNettyStreamClient client =
        new HttpClientBuilder(_eventLoop, _scheduler)
            .setJmxManager(manager)
            .build();
    // test setPoolStatsProvider
    try
    {
      setLatch.await(30, TimeUnit.SECONDS);
    }
    catch (InterruptedException e)
    {
      Assert.fail("PoolStatsAware setPoolStatsProvider didn't get called when creating channel pool.");
    }
    // test removePoolStatsProvider
    FutureCallback<None> shutdownCallback = new FutureCallback<None>();
    client.shutdown(shutdownCallback);
    try
    {
      removeLatch.await(30, TimeUnit.SECONDS);
    }
    catch (InterruptedException e)
    {
      Assert.fail("PoolStatsAware removePoolStatsProvider didn't get called when shutting down channel pool.");
    }
    shutdownCallback.get(30, TimeUnit.SECONDS);
  }

  @Test (enabled = false)
  public void testMakingOutboundHttpsRequest()
      throws NoSuchAlgorithmException, InterruptedException, ExecutionException, TimeoutException
  {
    SSLContext context = SSLContext.getDefault();
    SSLParameters sslParameters = context.getDefaultSSLParameters();

    HttpNettyStreamClient client = new HttpClientBuilder(_eventLoop, _scheduler)
          .setSSLContext(context)
          .setSSLParameters(sslParameters)
          .build();

    RestRequest r = new RestRequestBuilder(URI.create("https://www.howsmyssl.com/a/check")).build();
    FutureCallback<StreamResponse> cb = new FutureCallback<StreamResponse>();
    TransportCallback<StreamResponse> callback = new TransportCallbackAdapter<StreamResponse>(cb);
    client.streamRequest(Messages.toStreamRequest(r), new RequestContext(), new HashMap<String, String>(), callback);
    cb.get(30, TimeUnit.SECONDS);
  }

  private static class NoCreations implements ChannelPoolFactory
  {
    private final ScheduledExecutorService _scheduler;

    public NoCreations(ScheduledExecutorService scheduler)
    {
      _scheduler = scheduler;
    }

    @Override
    public AsyncPool<Channel> getPool(SocketAddress address)
    {
      return new AsyncPoolImpl<Channel>("fake pool", new AsyncPool.Lifecycle<Channel>()
      {
        @Override
        public void create(Callback<Channel> channelCallback)
        {

        }

        @Override
        public boolean validateGet(Channel obj)
        {
          return false;
        }

        @Override
        public boolean validatePut(Channel obj)
        {
          return false;
        }

        @Override
        public void destroy(Channel obj, boolean error, Callback<Channel> channelCallback)
        {

        }

        @Override
        public PoolStats.LifecycleStats getStats()
        {
          return null;
        }
      }, 0, 0, _scheduler);
    }

  }

}
