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

import java.io.IOException;
import java.net.SocketAddress;
import java.net.URI;
import java.nio.channels.UnresolvedAddressException;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.CountDownLatch;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;

import com.linkedin.r2.message.rest.Messages;
import com.linkedin.r2.message.rest.StreamResponse;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.handler.codec.frame.TooLongFrameException;
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
import com.linkedin.r2.message.rest.RestResponse;
import com.linkedin.r2.transport.common.bridge.client.TransportCallbackAdapter;
import com.linkedin.r2.transport.common.bridge.client.TransportClient;
import com.linkedin.r2.transport.common.bridge.common.TransportCallback;

/**
 * @author Steven Ihde
 * @version $Revision: $
 */

public class TestHttpNettyClient
{
  private NioClientSocketChannelFactory _factory;
  private ScheduledExecutorService _scheduler;

  private static final int TEST_MAX_RESPONSE_SIZE = 500000;

  private static final int RESPONSE_OK = 1;
  private static final int TOO_LARGE = 2;

  @BeforeClass
  public void setup()
  {
    _factory = new NioClientSocketChannelFactory(
            Executors.newCachedThreadPool(),
            Executors.newCachedThreadPool());
    _scheduler = Executors.newSingleThreadScheduledExecutor();
  }

  @AfterClass
  public void tearDown()
  {
    _factory.releaseExternalResources();
    _scheduler.shutdown();
  }

  @Test
  public void testNoChannelTimeout() throws InterruptedException
  {
    HttpNettyClient client = new HttpNettyClient(new NoCreations(_scheduler), _scheduler, 500, 500, 1024*1024*2);

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
  public void testNoResponseTimeout() throws InterruptedException, IOException
  {
    TestServer testServer = new TestServer();

    HttpNettyClient client = new HttpNettyClient(_factory, _scheduler, 1, 500, 10000, 500, 1024*1024*2);

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
    HttpNettyClient client = new HttpNettyClient(_factory, _scheduler, 1, 30000, 10000, 500,1024*1024*2);

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
      verifyCauseChain(e, RemoteInvocationException.class, UnresolvedAddressException.class);
    }
  }

  @Test
  public void testMaxResponseSize() throws InterruptedException, IOException, TimeoutException
  {
    testResponseSize(TEST_MAX_RESPONSE_SIZE-1, RESPONSE_OK);

    testResponseSize(TEST_MAX_RESPONSE_SIZE, RESPONSE_OK);

    testResponseSize(TEST_MAX_RESPONSE_SIZE+1, TOO_LARGE);
  }


  public void testResponseSize(int responseSize, int expectedResult) throws InterruptedException, IOException, TimeoutException
  {
    TestServer testServer = new TestServer();

    HttpNettyClient client = new HttpNettyClient(_factory, _scheduler, 1, 50000, 10000, 500, TEST_MAX_RESPONSE_SIZE);

    RestRequest r = new RestRequestBuilder(testServer.getResponseOfSizeURI(responseSize)).build();
    FutureCallback<StreamResponse> cb = new FutureCallback<StreamResponse>();
    TransportCallback<StreamResponse> callback = new TransportCallbackAdapter<StreamResponse>(cb);
    client.streamRequest(Messages.toStreamRequest(r), new RequestContext(), new HashMap<String, String>(), callback);

    try
    {
      cb.get(30, TimeUnit.SECONDS);
      if(expectedResult == TOO_LARGE)
      {
        Assert.fail("Max response size exceeded, expected exception. ");
      }
    }
    catch (ExecutionException e)
    {
      if(expectedResult == RESPONSE_OK)
      {
        Assert.fail("Unexpected ExecutionException, response was <= max response size.");
      }
      verifyCauseChain(e, RemoteInvocationException.class, TooLongFrameException.class);

    }
    testServer.shutdown();
  }

  @Test
  public void testShutdown() throws ExecutionException, TimeoutException, InterruptedException
  {
    TransportClient client = new HttpClientFactory().getClient(
            Collections.<String, String>emptyMap());

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
  private void testShutdownStuckInPool() throws InterruptedException, ExecutionException, TimeoutException

  {
    // Test that shutdown works when the outstanding request is stuck in the pool waiting for a channel
    HttpNettyClient client = new HttpNettyClient(new NoCreations(_scheduler), _scheduler, 60000, 1,1024*1024*2);

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
    testShutdownRequestOutstanding(500, 60000,
                                   RemoteInvocationException.class,
                                   TimeoutException.class);
  }

  @Test
  public void testShutdownRequestOutstanding2()
          throws IOException, ExecutionException, TimeoutException, InterruptedException
  {
    // Test that it works when the request timeout kills the outstanding request...
    testShutdownRequestOutstanding(60000, 500,
                                   RemoteInvocationException.class,
                                   // sometimes the test fails with ChannelClosedException
                                   // TimeoutException.class
                                   Exception.class
                                   );

  }

  private void testShutdownRequestOutstanding(int shutdownTimeout, int requestTimeout,
                                              Class<?>... causeChain)
          throws InterruptedException, IOException, ExecutionException, TimeoutException
  {
    TestServer testServer = new TestServer();

    HttpNettyClient client = new HttpNettyClient(_factory, _scheduler, 1, requestTimeout, 10000, shutdownTimeout, 1024*1024*2);

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
  public void testClientPipelineFactory1() throws NoSuchAlgorithmException
  {
    try
    {
      new HttpNettyClient(_factory, _scheduler, 1, 1, 1, 1, 1, null, new SSLParameters(), _scheduler, Integer.MAX_VALUE);
    }
    catch (IllegalArgumentException e)
    {
      // Check exception message to make sure it's the expected one.
      Assert.assertEquals(e.getMessage(),
                          "SSLParameters passed with no SSLContext");
    }
  }

  // Test that cannot set cipher suites in SSLParameters that don't have any match in
  // SSLContext.
  // This in fact tests HttpClientPipelineFactory constructor through HttpNettyClient
  // constructor.
  @Test
  public void testClientPipelineFactory2Fail() throws NoSuchAlgorithmException
  {
    String[] requestedCipherSuites = { "Unsupported" };
    SSLParameters sslParameters = new SSLParameters();
    sslParameters.setCipherSuites(requestedCipherSuites);
    try
    {
      new HttpNettyClient(_factory,
                          _scheduler,
                          1,
                          1,
                          1,
                          1,
                          1,
                          SSLContext.getDefault(),
                          sslParameters,
                          _scheduler,
                          Integer.MAX_VALUE);
    }
    catch (IllegalArgumentException e)
    {
      // Check exception message to make sure it's the expected one.
      Assert.assertEquals(e.getMessage(),
                          "None of the requested cipher suites: [Unsupported] are found in SSLContext");
    }
  }

  // Test that can set cipher suites in SSLParameters that have at least one match in
  // SSLContext.
  // This in fact tests HttpClientPipelineFactory constructor through HttpNettyClient
  // constructor.
  @Test
  public void testClientPipelineFactory2Pass() throws NoSuchAlgorithmException
  {
    String[] requestedCipherSuites = { "Unsupported", "SSL_RSA_WITH_RC4_128_SHA" };
    SSLParameters sslParameters = new SSLParameters();
    sslParameters.setCipherSuites(requestedCipherSuites);
    new HttpNettyClient(_factory,
                        _scheduler,
                        1,
                        1,
                        1,
                        1,
                        1,
                        SSLContext.getDefault(),
                        sslParameters,
                        _scheduler,
                        Integer.MAX_VALUE);
  }

  // Test that cannot set protocols in SSLParameters that don't have any match in
  // SSLContext.
  // This in fact tests HttpClientPipelineFactory constructor through HttpNettyClient
  // constructor.
  @Test
  public void testClientPipelineFactory3Fail() throws NoSuchAlgorithmException
  {
    String[] requestedProtocols = { "Unsupported" };
    SSLParameters sslParameters = new SSLParameters();
    sslParameters.setProtocols(requestedProtocols);
    try
    {
      new HttpNettyClient(_factory,
                          _scheduler,
                          1,
                          1,
                          1,
                          1,
                          1,
                          SSLContext.getDefault(),
                          sslParameters,
                          _scheduler,
                          Integer.MAX_VALUE);
    }
    catch (IllegalArgumentException e)
    {
      // Check exception message to make sure it's the expected one.
      Assert.assertEquals(e.getMessage(),
                          "None of the requested protocols: [Unsupported] are found in SSLContext");
    }
  }

  // Test that can set protocols in SSLParameters that have at least one match in
  // SSLContext.
  // This in fact tests HttpClientPipelineFactory constructor through HttpNettyClient
  // constructor.
  @Test
  public void testClientPipelineFactory3Pass() throws NoSuchAlgorithmException
  {
    String[] requestedProtocols = { "Unsupported", "TLSv1" };
    SSLParameters sslParameters = new SSLParameters();
    sslParameters.setProtocols(requestedProtocols);
    new HttpNettyClient(_factory,
                        _scheduler,
                        1,
                        1,
                        1,
                        1,
                        1,
                        SSLContext.getDefault(),
                        sslParameters,
                        _scheduler,
                        Integer.MAX_VALUE);
  }

  @Test
  public void testPoolStatsProviderManager() throws InterruptedException, ExecutionException, TimeoutException
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

    HttpNettyClient client = new HttpNettyClient(_factory,
                                                  _scheduler,
                                                  1,
                                                  1,
                                                  1,
                                                  1000,
                                                  1,
                                                  null,
                                                  null,
                                                  _scheduler,
                                                  Integer.MAX_VALUE,
                                                  HttpClientFactory.DEFAULT_CLIENT_NAME,
                                                  manager);
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

  /*
  //@Test
  public void testFullPoolTimeout()
  {
    AsyncPool<Object> pool = new AsyncPoolImpl<Object>("object pool",
                                                       new TestAsyncPool.SynchronousLifecycle(),
                                                       1,
                                                       100,
                                                       _executor
                                                       );
    pool.start();
    FutureCallback<Object> cb1 = new FutureCallback<Object>();
    pool.get(cb1);
    try
    {
      Object o = cb1.getRaw();
      Assert.assertNotNull(o);
    }
    catch (Exception e)
    {
      Assert.fail("getResponse failed unexpectedly", e);
    }

    FutureCallback<Object> cb2 = new FutureCallback<Object>();
    // This get should fail since pool size is one and previous object not returned
    pool.get(cb2);

    try
    {
      Object o = cb2.getRaw();
      Assert.fail("Get was supposed to time out");
    }
    catch (TimeoutException e)
    {
      // correct
    }
    catch (Exception e)
    {
      Assert.fail("Failed with unexpected exception instead of TimeoutException", e);
    }

  }
*/
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
