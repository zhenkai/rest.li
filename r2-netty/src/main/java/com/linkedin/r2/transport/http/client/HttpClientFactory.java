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

/* $Id$ */
package com.linkedin.r2.transport.http.client;


import com.linkedin.common.callback.Callback;
import com.linkedin.common.util.None;
import com.linkedin.r2.filter.FilterChain;
import com.linkedin.r2.filter.FilterChains;
import com.linkedin.r2.filter.CompressionConfig;
import com.linkedin.r2.filter.compression.ClientCompressionFilter;
import com.linkedin.r2.filter.compression.ClientStreamCompressionFilter;
import com.linkedin.r2.filter.compression.EncodingType;
import com.linkedin.r2.filter.transport.ClientQueryTunnelFilter;
import com.linkedin.r2.filter.transport.FilterChainClient;
import com.linkedin.r2.message.RequestContext;
import com.linkedin.r2.message.rest.RestRequest;
import com.linkedin.r2.message.rest.RestResponse;
import com.linkedin.r2.message.stream.StreamRequest;
import com.linkedin.r2.message.stream.StreamResponse;
import com.linkedin.r2.transport.common.TransportClientFactory;
import com.linkedin.r2.transport.common.bridge.client.TransportClient;
import com.linkedin.r2.transport.common.bridge.common.TransportCallback;
import com.linkedin.r2.util.ConfigValueExtractor;
import com.linkedin.r2.util.NamedThreadFactory;

import io.netty.channel.nio.NioEventLoopGroup;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A factory for HttpNettyClient instances.
 *
 * All clients created by the factory will share the same resources, in particular the
 * {@link io.netty.channel.nio.NioEventLoopGroup} and {@link ScheduledExecutorService}.
 *
 * In order to shutdown cleanly, all clients issued by the factory should be shutdown via
 * {@link TransportClient#shutdown(com.linkedin.common.callback.Callback)} and the factory
 * itself should be shut down via one of the following two methods:
 * <ul>
 * <li>{@link #shutdown(com.linkedin.common.callback.Callback)}</li>
 * <li>
 * {@link #shutdown(com.linkedin.common.callback.Callback, long, java.util.concurrent.TimeUnit)}
 * </li>
 * </ul>
 *
 * See the method descriptions for more details. Note that factory shutdown and shutdown
 * of the clients can be initiated in any order.
 *
 * @author Chris Pettitt
 * @author Steven Ihde
 * @version $Revision$
 */
public class HttpClientFactory implements TransportClientFactory
{
  private static final Logger LOG = LoggerFactory.getLogger(HttpClientFactory.class);

  public static final String HTTP_QUERY_POST_THRESHOLD = "http.queryPostThreshold";
  public static final String HTTP_REQUEST_TIMEOUT = "http.requestTimeout";
  public static final String HTTP_MAX_RESPONSE_SIZE = "http.maxResponseSize";
  public static final String HTTP_POOL_SIZE = "http.poolSize";
  public static final String HTTP_POOL_WAITER_SIZE = "http.poolWaiterSize";
  public static final String HTTP_IDLE_TIMEOUT = "http.idleTimeout";
  public static final String HTTP_SHUTDOWN_TIMEOUT = "http.shutdownTimeout";
  public static final String HTTP_SSL_CONTEXT = "http.sslContext";
  public static final String HTTP_SSL_PARAMS = "http.sslParams";
  public static final String HTTP_RESPONSE_COMPRESSION_OPERATIONS = "http.responseCompressionOperations";
  public static final String HTTP_REQUEST_CONTENT_ENCODINGS = "http.requestContentEncodings";
  public static final String HTTP_SERVICE_NAME = "http.serviceName";
  public static final String HTTP_POOL_STRATEGY = "http.poolStrategy";
  public static final String HTTP_POOL_MIN_SIZE = "http.poolMinSize";
  public static final String HTTP_MAX_HEADER_SIZE = "http.maxHeaderSize";
  public static final String HTTP_MAX_CHUNK_SIZE = "http.maxChunkSize";
  public static final String HTTP_MAX_CONCURRENT_CONNECTIONS = "http.maxConcurrentConnections";

  public static final int DEFAULT_POOL_WAITER_SIZE = Integer.MAX_VALUE;
  public static final int DEFAULT_POOL_SIZE = 200;
  public static final int DEFAULT_REQUEST_TIMEOUT = 10000;
  public static final int DEFAULT_IDLE_TIMEOUT = 25000;
  public static final int DEFAULT_SHUTDOWN_TIMEOUT = 5000;
  public static final long DEFAULT_MAX_RESPONSE_SIZE = 1024 * 1024 * 2;
  public static final String DEFAULT_CLIENT_NAME = "noNameSpecifiedClient";
  public static final AsyncPoolImpl.Strategy DEFAULT_POOL_STRATEGY = AsyncPoolImpl.Strategy.MRU;
  public static final int DEFAULT_POOL_MIN_SIZE = 0;
  public static final int DEFAULT_MAX_HEADER_SIZE = 8 * 1024;
  public static final int DEFAULT_MAX_CHUNK_SIZE = 8 * 1024;


  private static final String LIST_SEPARATOR = ",";

  private final NioEventLoopGroup          _eventLoopGroup;
  private final ScheduledExecutorService   _executor;
  private final ExecutorService            _callbackExecutorGroup;
  private final boolean                    _shutdownFactory;
  private final boolean                    _shutdownExecutor;
  private final boolean                    _shutdownCallbackExecutor;
  private final FilterChain                _filters;
  private final boolean                    _useClientCompression;
  private final Executor                   _compressionExecutor;

  private final AtomicBoolean              _finishingShutdown = new AtomicBoolean(false);
  private volatile ScheduledFuture<?>      _shutdownTimeoutTask;
  private final AbstractJmxManager         _jmxManager;

  /** Default request compression config (used when a config for a service isn't specified in {@link #_requestCompressionConfigs}) */
  private final CompressionConfig          _defaultRequestCompressionConfig;
  /** Request compression config for each http service. */
  private final Map<String, CompressionConfig> _requestCompressionConfigs;
  // flag to enable/disable Nagle's algorithm
  private final boolean                    _tcpNoDelay;

  // All fields below protected by _mutex
  private final Object                     _mutex               = new Object();
  private boolean                          _running             = true;
  private int                              _clientsOutstanding  = 0;
  private Callback<None>                   _factoryShutdownCallback;

  /**
   * Construct a new instance using an empty filter chain.
   */
  public HttpClientFactory()
  {
    this(FilterChains.empty());
  }

  /**
   * Construct a new instance with a specified callback executor.
   *
   * @param callbackExecutor an optional executor to invoke user callbacks that otherwise
   *          will be invoked by scheduler executor.
   * @param shutdownCallbackExecutor if true, the callback executor will be shut down when
   *          this factory is shut down
   */
  public HttpClientFactory(ExecutorService callbackExecutor,
                           boolean shutdownCallbackExecutor)
  {
    this(FilterChains.empty(),
         new NioEventLoopGroup(0 /* use default settings */, new NamedThreadFactory("R2 Nio Event Loop")),
         true,
         Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("R2 Netty Scheduler")),
         true,
         callbackExecutor,
         shutdownCallbackExecutor);
  }

  /**
   * Construct a new instance using the specified filter chain.
   *
   * @param filters the {@link FilterChain} shared by all Clients created by this factory.
   */
  public HttpClientFactory(FilterChain filters)
  {
    // TODO Disable Netty's thread renaming so that the names below are the ones that actually
    // show up in log messages; need to coordinate with Espresso team (who also have netty threads)
    this(filters,
         new NioEventLoopGroup(0 /* use default settings */, new NamedThreadFactory("R2 Nio Event Loop")),
         true,
         Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("R2 Netty Scheduler")),
         true);
  }

  /**
   * Creates a new HttpClientFactory.
   *
   * @param filters the filter chain shared by all Clients created by this factory
   * @param eventLoopGroup the {@link NioEventLoopGroup} that all Clients created by this
   *          factory will share
   * @param shutdownFactory if true, the channelFactory will be shut down when this
   *          factory is shut down
   * @param executor an executor shared by all Clients created by this factory to schedule
   *          tasks
   * @param shutdownExecutor if true, the executor will be shut down when this factory is
   *          shut down
   */
  public HttpClientFactory(FilterChain filters,
                           NioEventLoopGroup eventLoopGroup,
                           boolean shutdownFactory,
                           ScheduledExecutorService executor,
                           boolean shutdownExecutor)
  {
    this(filters,
         eventLoopGroup,
         shutdownFactory,
         executor,
         shutdownExecutor,
         null,
         false);
  }

  /**
   * Creates a new HttpClientFactory.
   *
   * @param filters the filter chain shared by all Clients created by this factory
   * @param eventLoopGroup the {@link NioEventLoopGroup} that all Clients created by this
   *          factory will share
   * @param shutdownFactory if true, the channelFactory will be shut down when this
   *          factory is shut down
   * @param executor an executor shared by all Clients created by this factory to schedule
   *          tasks
   * @param shutdownExecutor if true, the executor will be shut down when this factory is
   *          shut down
   * @param callbackExecutorGroup an optional executor group to execute user callbacks that otherwise
   *          will be executed by eventLoopGroup.
   * @param shutdownCallbackExecutor if true, the callback executor will be shut down when
   *          this factory is shut down
   */
  public HttpClientFactory(FilterChain filters,
                           NioEventLoopGroup eventLoopGroup,
                           boolean shutdownFactory,
                           ScheduledExecutorService executor,
                           boolean shutdownExecutor,
                           ExecutorService callbackExecutorGroup,
                           boolean shutdownCallbackExecutor)
  {
    this(filters,
         eventLoopGroup,
         shutdownFactory,
         executor,
         shutdownExecutor,
         callbackExecutorGroup,
         shutdownCallbackExecutor,
         AbstractJmxManager.NULL_JMX_MANAGER);
  }

  public HttpClientFactory(FilterChain filters,
                           NioEventLoopGroup eventLoopGroup,
                           boolean shutdownFactory,
                           ScheduledExecutorService executor,
                           boolean shutdownExecutor,
                           ExecutorService callbackExecutorGroup,
                           boolean shutdownCallbackExecutor,
                           AbstractJmxManager jmxManager)
  {
    this(filters, eventLoopGroup, shutdownFactory, executor, shutdownExecutor, callbackExecutorGroup,
        shutdownCallbackExecutor, jmxManager, true);
  }


  public HttpClientFactory(FilterChain filters,
                           NioEventLoopGroup eventLoopGroup,
                           boolean shutdownFactory,
                           ScheduledExecutorService executor,
                           boolean shutdownExecutor,
                           ExecutorService callbackExecutorGroup,
                           boolean shutdownCallbackExecutor,
                           AbstractJmxManager jmxManager,
                           boolean tcpNoDelay)
  {
    this(filters, eventLoopGroup, shutdownFactory, executor, shutdownExecutor, callbackExecutorGroup, shutdownCallbackExecutor,
        jmxManager, tcpNoDelay, Integer.MAX_VALUE, Collections.<String, CompressionConfig>emptyMap(), null);
  }

  public HttpClientFactory(FilterChain filters,
                           NioEventLoopGroup eventLoopGroup,
                           boolean shutdownFactory,
                           ScheduledExecutorService executor,
                           boolean shutdownExecutor,
                           ExecutorService callbackExecutorGroup,
                           boolean shutdownCallbackExecutor,
                           AbstractJmxManager jmxManager,
                           boolean tcpNoDelay,
                           int requestCompressionThresholdDefault,
                           Map<String, CompressionConfig> requestCompressionConfigs,
                           Executor compressionExecutor)
  {
    _filters = filters;
    _eventLoopGroup = eventLoopGroup;
    _shutdownFactory = shutdownFactory;
    _executor = executor;
    _shutdownExecutor = shutdownExecutor;
    _callbackExecutorGroup = callbackExecutorGroup;
    _shutdownCallbackExecutor = shutdownCallbackExecutor;
    _jmxManager = jmxManager;
    if (requestCompressionThresholdDefault < 0)
    {
      throw new IllegalArgumentException("requestCompressionThresholdDefault should not be negative.");
    }
    _defaultRequestCompressionConfig = new CompressionConfig(requestCompressionThresholdDefault);
    if (requestCompressionConfigs == null)
    {
      throw new IllegalArgumentException("requestCompressionConfigs should not be null.");
    }
    _requestCompressionConfigs = Collections.unmodifiableMap(requestCompressionConfigs);
    _tcpNoDelay = tcpNoDelay;
    _compressionExecutor = compressionExecutor;
    _useClientCompression = _compressionExecutor != null;
  }

  public static class Builder
  {
    private NioEventLoopGroup          _eventLoopGroup = null;
    private ScheduledExecutorService   _executor = null;
    private ExecutorService            _callbackExecutorGroup = null;
    private boolean                    _shutdownFactory = true;
    private boolean                    _shutdownExecutor = true;
    private boolean                    _shutdownCallbackExecutor = false;
    private FilterChain                _filters = FilterChains.empty();
    private Executor                   _compressionExecutor = null;
    private AbstractJmxManager         _jmxManager = AbstractJmxManager.NULL_JMX_MANAGER;

    private int                        _requestCompressionThresholdDefault = Integer.MAX_VALUE;
    private Map<String, CompressionConfig> _requestCompressionConfigs = Collections.<String, CompressionConfig>emptyMap();
    private boolean                    _tcpNoDelay = true;

    public Builder setNioEventLoopGroup(NioEventLoopGroup nioEventLoopGroup)
    {
      _eventLoopGroup = nioEventLoopGroup;
      return this;
    }

    public Builder setScheduleExecutorService(ScheduledExecutorService scheduleExecutorService)
    {
      _executor = scheduleExecutorService;
      return this;
    }

    public Builder setCallbackExecutor(ExecutorService callbackExecutor)
    {
      _callbackExecutorGroup = callbackExecutor;
      return this;
    }

    public Builder setShutDownFactory(boolean shutDownFactory)
    {
      _shutdownFactory = shutDownFactory;
      return this;
    }

    public Builder setShutdownScheduledExecutorService(boolean shutdown)
    {
      _shutdownExecutor = shutdown;
      return this;
    }

    public Builder setShutdownCallbackExecutor(boolean shutdown)
    {
      _shutdownCallbackExecutor = shutdown;
      return this;
    }

    public Builder setFilterChain(FilterChain filterChain)
    {
      _filters = filterChain;
      return this;
    }

    public Builder setCompressionExecutor(Executor executor)
    {
      _compressionExecutor = executor;
      return this;
    }

    public Builder setJmxManager(AbstractJmxManager jmxManager)
    {
      _jmxManager = jmxManager;
      return this;
    }

    public Builder setRequestCompressionThresholdDefault(int thresholdDefault)
    {
      _requestCompressionThresholdDefault = thresholdDefault;
      return this;
    }

    public Builder setRequestCompressionConfigs(Map<String, CompressionConfig> configs)
    {
      _requestCompressionConfigs = configs;
      return this;
    }

    public Builder setTcpNoDelay(boolean tcpNoDelay)
    {
      _tcpNoDelay = tcpNoDelay;
      return this;
    }

    public HttpClientFactory build()
    {
      NioEventLoopGroup eventLoopGroup = _eventLoopGroup != null ? _eventLoopGroup
          : new NioEventLoopGroup(0 /* use default settings */, new NamedThreadFactory("R2 Nio Event Loop"));
      ScheduledExecutorService scheduledExecutorService = _executor != null ? _executor
          : Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("R2 Netty Scheduler"));

      return new HttpClientFactory(_filters, eventLoopGroup, _shutdownFactory, scheduledExecutorService,
          _shutdownExecutor, _callbackExecutorGroup, _shutdownCallbackExecutor, _jmxManager,
          _tcpNoDelay, _requestCompressionThresholdDefault, _requestCompressionConfigs, _compressionExecutor);
    }

  }

  @Override
  public TransportClient getClient(Map<String, ? extends Object> properties)
  {
    SSLContext sslContext;
    SSLParameters sslParameters;

    // Copy the properties map since we don't want to mutate the passed-in map by removing keys
    properties = new HashMap<String,Object>(properties);
    sslContext = coerceAndRemoveFromMap(HTTP_SSL_CONTEXT, properties, SSLContext.class);
    sslParameters = coerceAndRemoveFromMap(HTTP_SSL_PARAMS, properties, SSLParameters.class);

    return getClient(properties, sslContext, sslParameters);
  }

  TransportClient getRawClient(Map<String, String> properties)
  {
    return getRawClient(properties, null, null);
  }

  private static <T> T coerceAndRemoveFromMap(String key, Map<String, ?> props, Class<T> valueClass)
  {
    return coerce(key, props.remove(key), valueClass);
  }

  private static <T> T coerce(String key, Object value, Class<T> valueClass)
  {
    if (value == null)
    {
      return null;
    }
    if (!valueClass.isInstance(value))
    {
      throw new IllegalArgumentException(
              "Property " + key + " is of type " + value.getClass().getName() +
              " but must be " + valueClass.getName());
    }
    return valueClass.cast(value);
  }

  /* package private */ CompressionConfig getCompressionConfig(String httpServiceName, String requestContentEncodingName)
  {
    if (_requestCompressionConfigs.containsKey(httpServiceName))
    {
      if (requestContentEncodingName == EncodingType.IDENTITY.getHttpName())
      {
        // This will likely happen when the service doesn't allow any request content encodings for compression,
        // but the client specified a compression config for the service.
        // The client probably has a misunderstanding (thinks the service supports request compression when it actually does not).
        // Note that it is okay to pass in any compression config to ClientCompressionFilter when there isn't an available algorithm
        // because ClientCompressionFilter will not compress requests when encoding type is IDENTITY.
        LOG.warn("No request compression algorithm available but compression config specified for service {}", httpServiceName);
      }
      return _requestCompressionConfigs.get(httpServiceName);
    }
    return _defaultRequestCompressionConfig;
  }

  /**
   * Create a new {@link TransportClient} with the specified properties,
   * {@link SSLContext} and {@link SSLParameters}
   *
   * @param properties map of properties for the {@link TransportClient}
   * @param sslContext {@link SSLContext} to be used for requests over SSL/TLS.
   * @param sslParameters {@link SSLParameters} to configure secure connections.
   * @return an appropriate {@link TransportClient} instance, as specified by the properties.
   */
  private TransportClient getClient(Map<String, ? extends Object> properties,
                                   SSLContext sslContext,
                                   SSLParameters sslParameters)
  {
    LOG.info("Getting a client with configuration {} and SSLContext {}",
             properties,
             sslContext);
    TransportClient client = getRawClient(properties, sslContext, sslParameters);

    List<String> httpResponseCompressionOperations = ConfigValueExtractor.buildList(properties.remove(HTTP_RESPONSE_COMPRESSION_OPERATIONS),
                                                                                    LIST_SEPARATOR);
    List<String> httpRequestServerSupportedEncodings = ConfigValueExtractor.buildList(properties.remove(HTTP_REQUEST_CONTENT_ENCODINGS),
                                                                                      LIST_SEPARATOR);
    FilterChain filters;

    if (_useClientCompression)
    {
      // add compression filter for stream messages
      String httpServiceName = (String) properties.get(HTTP_SERVICE_NAME);
      String requestContentEncodingName = getStreamRequestContentEncodingName(httpRequestServerSupportedEncodings);
      CompressionConfig compressionConfig = getCompressionConfig(httpServiceName, requestContentEncodingName);
      String responseCompressionSchemaName = httpResponseCompressionOperations.isEmpty() ? "" : buildStreamAcceptEncodingSchemaNames();
      filters = _filters.addLast(new ClientStreamCompressionFilter(requestContentEncodingName,
          compressionConfig,
          responseCompressionSchemaName,
          httpResponseCompressionOperations,
          _compressionExecutor));

      requestContentEncodingName = getRestRequestContentEncodingName(httpRequestServerSupportedEncodings);
      responseCompressionSchemaName = httpResponseCompressionOperations.isEmpty() ? "" : buildRestAcceptEncodingSchemaNames();
      filters = filters.addLast(new ClientCompressionFilter(requestContentEncodingName,
          compressionConfig,
          responseCompressionSchemaName,
          httpResponseCompressionOperations));
    }
    else
    {
      filters = _filters;
    }

    Integer queryPostThreshold = chooseNewOverDefault(getIntValue(properties, HTTP_QUERY_POST_THRESHOLD), Integer.MAX_VALUE);
    filters = filters.addLast(new ClientQueryTunnelFilter(queryPostThreshold));

    client = new FilterChainClient(client, filters);
    client = new FactoryClient(client);
    synchronized (_mutex)
    {
      if (!_running)
      {
        throw new IllegalStateException("Factory is shutting down");
      }
      _clientsOutstanding++;
      return client;
    }
  }

  /**
   * Chooses the first encoding in the given list of supported encodings that the client can compress with.
   * This assumes that the service listed the encodings in order of preference.
   *
   * @param serverSupportedEncodings list of compression encodings the server supports.
   * @return the encoding name that should be used to compress requests.
   */
  private static String getStreamRequestContentEncodingName(List<String> serverSupportedEncodings)
  {
    for (String encoding: serverSupportedEncodings)
    {
      if (com.linkedin.r2.filter.compression.streaming.EncodingType.isSupported(encoding))
      {
        return encoding;
      }
    }
    return com.linkedin.r2.filter.compression.streaming.EncodingType.IDENTITY.getHttpName();
  }

  /**
   * Chooses the first encoding in the given list of supported encodings that the client can compress with.
   * This assumes that the service listed the encodings in order of preference.
   *
   * @param serverSupportedEncodings list of compression encodings the server supports.
   * @return the encoding name that should be used to compress requests.
   */
  private static String getRestRequestContentEncodingName(List<String> serverSupportedEncodings)
  {
    for (String encoding: serverSupportedEncodings)
    {
      if (EncodingType.isSupported(encoding))
      {
        return encoding;
      }
    }
    return EncodingType.IDENTITY.getHttpName();
  }

  /**
   * @return the compression schemas that the client will support for response compression
   */
  private String buildStreamAcceptEncodingSchemaNames()
  {
    List<String> schemaNames = new ArrayList<String>();
    for (com.linkedin.r2.filter.compression.streaming.EncodingType type: com.linkedin.r2.filter.compression.streaming.EncodingType.values())
    {
      // For now clients will accept all supported encodings (which is why we don't add EncodingType.ANY as an accepted
      // type)
      if (!type.equals(com.linkedin.r2.filter.compression.streaming.EncodingType.IDENTITY) && !type.equals(com.linkedin.r2.filter.compression.streaming.EncodingType.ANY))
      {
        schemaNames.add(type.getHttpName());
      }
    }
    return StringUtils.join(schemaNames, ",");
  }

  /**
   * @return the compression schemas that the client will support for response compression
   */
  private String buildRestAcceptEncodingSchemaNames()
  {
    List<String> schemaNames = new ArrayList<String>();
    for (EncodingType type: EncodingType.values())
    {
      // For now clients will accept all supported encodings (which is why we don't add EncodingType.ANY as an accepted
      // type)
      if (!type.equals(EncodingType.IDENTITY) && !type.equals(EncodingType.ANY))
      {
        schemaNames.add(type.getHttpName());
      }
    }
    return StringUtils.join(schemaNames, ",");
  }

  /**
   * helper method to get value from properties as well as to print log warning if the key is old
   * @param properties
   * @param propertyKey
   * @return null if property key can't be found, integer otherwise
   */
  private Integer getIntValue(Map<String, ? extends Object> properties, String propertyKey)
  {
    if (properties == null)
    {
      LOG.warn("passed a null raw client properties");
      return null;
    }
    if (properties.containsKey(propertyKey))
    {
      // These properties can be safely cast to String before converting them to Integers as we expect Integer values
      // for all these properties.
      return Integer.parseInt((String) properties.get(propertyKey));
    }
    else
    {
      return null;
    }
  }

  /**
   * helper method to get value from properties as well as to print log warning if the key is old
   * @param properties
   * @param propertyKey
   * @return null if property key can't be found, integer otherwise
   */
  private Long getLongValue(Map<String, ? extends Object> properties, String propertyKey)
  {
    if (properties == null)
    {
      LOG.warn("passed a null raw client properties");
      return null;
    }
    if (properties.containsKey(propertyKey))
    {
      // These properties can be safely cast to String before converting them to Integers as we expect Integer values
      // for all these properties.
      return Long.parseLong((String)properties.get(propertyKey));
    }
    else
    {
      return null;
    }
  }

  private AsyncPoolImpl.Strategy getStrategy(Map<String, ? extends Object> properties)
  {
    if (properties == null)
    {
      LOG.warn("passed a null raw client properties");
      return null;
    }
    if (properties.containsKey(HTTP_POOL_STRATEGY))
    {
      String strategyString = (String)properties.get(HTTP_POOL_STRATEGY);
      if (strategyString.equalsIgnoreCase("LRU"))
      {
        return AsyncPoolImpl.Strategy.LRU;
      }
      else if (strategyString.equalsIgnoreCase("MRU"))
      {
        return AsyncPoolImpl.Strategy.MRU;
      }
    }
    // for all other cases
    return null;
  }

  /**
   * Testing aid.
   */
  TransportClient getRawClient(Map<String, ? extends Object> properties,
                               SSLContext sslContext,
                               SSLParameters sslParameters)
  {
    Integer poolSize = chooseNewOverDefault(getIntValue(properties, HTTP_POOL_SIZE), DEFAULT_POOL_SIZE);
    Integer idleTimeout = chooseNewOverDefault(getIntValue(properties, HTTP_IDLE_TIMEOUT), DEFAULT_IDLE_TIMEOUT);
    Integer shutdownTimeout = chooseNewOverDefault(getIntValue(properties, HTTP_SHUTDOWN_TIMEOUT), DEFAULT_SHUTDOWN_TIMEOUT);
    long maxResponseSize = chooseNewOverDefault(getLongValue(properties, HTTP_MAX_RESPONSE_SIZE), DEFAULT_MAX_RESPONSE_SIZE);
    Integer requestTimeout = chooseNewOverDefault(getIntValue(properties, HTTP_REQUEST_TIMEOUT), DEFAULT_REQUEST_TIMEOUT);
    Integer poolWaiterSize = chooseNewOverDefault(getIntValue(properties, HTTP_POOL_WAITER_SIZE), DEFAULT_POOL_WAITER_SIZE);
    String clientName = null;
    if (properties != null && properties.containsKey(HTTP_SERVICE_NAME))
    {
      clientName = properties.get(HTTP_SERVICE_NAME) + "Client";
    }
    clientName = chooseNewOverDefault(clientName, DEFAULT_CLIENT_NAME);
    AsyncPoolImpl.Strategy strategy = chooseNewOverDefault(getStrategy(properties), DEFAULT_POOL_STRATEGY);
    Integer poolMinSize = chooseNewOverDefault(getIntValue(properties, HTTP_POOL_MIN_SIZE), DEFAULT_POOL_MIN_SIZE);
    Integer maxHeaderSize = chooseNewOverDefault(getIntValue(properties, HTTP_MAX_HEADER_SIZE), DEFAULT_MAX_HEADER_SIZE);
    Integer maxChunkSize = chooseNewOverDefault(getIntValue(properties, HTTP_MAX_CHUNK_SIZE), DEFAULT_MAX_CHUNK_SIZE);
    Integer maxConcurrentConnections = chooseNewOverDefault(getIntValue(properties, HTTP_MAX_CONCURRENT_CONNECTIONS), Integer.MAX_VALUE);

    HttpNettyStreamClient streamClient = new HttpNettyStreamClient(_eventLoopGroup,
      _executor,
      poolSize,
      requestTimeout,
      idleTimeout,
      shutdownTimeout,
      maxResponseSize,
      sslContext,
      sslParameters,
      _callbackExecutorGroup,
      poolWaiterSize,
      clientName + "-Stream",  // to distinguish channel pool metrics from rest client during transition period
      _jmxManager,
      strategy,
      poolMinSize,
      maxHeaderSize,
      maxChunkSize,
      maxConcurrentConnections,
      _tcpNoDelay);

    HttpNettyClient legacyClient = new HttpNettyClient(_eventLoopGroup,
        _executor,
        poolSize,
        requestTimeout,
        idleTimeout,
        shutdownTimeout,
        (int)maxResponseSize,
        sslContext,
        sslParameters,
        _callbackExecutorGroup,
        poolWaiterSize,
        clientName,
        _jmxManager,
        strategy,
        poolMinSize,
        maxHeaderSize,
        maxChunkSize,
        maxConcurrentConnections);

    return new SwitchableClient(legacyClient, streamClient);
  }

  /**
   * choose new value. If new value doesn't exist, choose default value.
   *
   * @param newValue
   * @param defaultValue
   */
  private <T> T chooseNewOverDefault(T newValue, T defaultValue)
  {
    if (newValue == null)
    {
      return defaultValue;
    }
    else
    {
      return newValue;
    }
  }


  /**
   * Initiates an orderly shutdown of the factory wherein no more clients will be created,
   * and the shutdown will complete when all existing clients have been shut down.  If some
   * clients fail to shutdown, the factory will never shut down.  Shutdown of the clients must
   * be initiated independently, but can occur before or after factory shutdown is initiated.
   *
   * After all clients have shut down, the ClientSocketChannelFactory and ScheduledExecutorService
   * will be shut down, if these options were selected at construction time.
   *
   * @param callback invoked after all outstanding clients and this factory have completed shutdown
   */
  @Override
  public void shutdown(final Callback<None> callback)
  {
    final int count;
    synchronized (_mutex)
    {
      _running = false;
      count = _clientsOutstanding;
      _factoryShutdownCallback = callback;
    }

    if (count == 0)
    {
      finishShutdown();
    }
    else
    {
      LOG.info("Awaiting shutdown of {} outstanding clients", count);
    }
  }

  /**
   * Initiates an orderly shutdown similar to
   * {@link #shutdown(com.linkedin.common.callback.Callback)}. However, in the case that
   * some clients fail to shutdown, the factory shutdown will still complete after the
   * specified timeout.
   *
   * @param callback invoked after all clients shutdown (or the timeout expires) and the
   *          factory has shut down
   * @param timeout the timeout
   * @param timeoutUnit the timeout unit
   */
  public void shutdown(Callback<None> callback, long timeout, TimeUnit timeoutUnit)
  {
    // Schedule a timeout in case shutdown does not happen normally
    _shutdownTimeoutTask = _executor.schedule(new Runnable()
    {
      @Override
      public void run()
      {
        LOG.warn("Shutdown timeout exceeded, proceeding with shutdown");
        finishShutdown();
      }
    }, timeout, timeoutUnit);

    // Initiate orderly shutdown
    shutdown(callback);
  }

  private void finishShutdown()
  {
    if (!_finishingShutdown.compareAndSet(false, true))
    {
      return;
    }
    if (_shutdownTimeoutTask != null)
    {
      _shutdownTimeoutTask.cancel(false);
    }

    if (_shutdownFactory)
    {
      LOG.info("Shutdown Netty Event Loop");
      _eventLoopGroup.shutdownGracefully(0, 0, TimeUnit.SECONDS);
    }

    if (_shutdownExecutor)
    {
      // Due to a bug in ScheduledThreadPoolExecutor, shutdownNow() returns cancelled
      // tasks as though they were still pending execution.  If the executor has a large
      // number of cancelled tasks, shutdownNow() could take a long time to copy the array
      // of tasks.  Calling shutdown() first will purge the cancelled tasks.  Bug filed with
      // Oracle; will provide bug number when available.  May be fixed in JDK7 already.
      _executor.shutdown();
      _executor.shutdownNow();
      LOG.info("Scheduler shutdown complete");
    }

    if (_shutdownCallbackExecutor)
    {
      LOG.info("Shutdown callback executor");
      _callbackExecutorGroup.shutdown();
      _callbackExecutorGroup.shutdownNow();
    }

    final Callback<None> callback;
    synchronized (_mutex)
    {
      callback = _factoryShutdownCallback;
    }

    LOG.info("Shutdown complete");
    callback.onSuccess(None.none());
  }

  private void clientShutdown()
  {
    final boolean done;
    synchronized (_mutex)
    {
      _clientsOutstanding--;
      done = !_running && _clientsOutstanding == 0;
    }
    if (done)
    {
      finishShutdown();
    }
  }

  /**
   * The FactoryClient is a wrapper that simply does reference counting for all clients
   * issued by this factory, so that we can know when all outstanding clients have been
   * shut down completely.
   *
   * It introduces no synchronization overhead in the per-request code path, only the
   * shutdown code path.
   */
  private class FactoryClient implements TransportClient
  {
    private final TransportClient _client;
    private final AtomicBoolean _shutdown = new AtomicBoolean(false);

    private FactoryClient(TransportClient client)
    {
      _client = client;
    }

    @Override
    public   void restRequest(RestRequest request,
                              RequestContext requestContext,
                              Map<String, String> wireAttrs,
                              TransportCallback<RestResponse> callback)
    {
      _client.restRequest(request, requestContext, wireAttrs, callback);
    }

    @Override
    public void streamRequest(StreamRequest request, RequestContext requestContext,
                            Map<String, String> wireAttrs,
                            TransportCallback<StreamResponse> callback)
    {
      _client.streamRequest(request, requestContext, wireAttrs, callback);
    }

    @Override
    public void shutdown(final Callback<None> callback)
    {
      if (_shutdown.compareAndSet(false, true))
      {
        _client.shutdown(new Callback<None>()
        {
          @Override
          public void onSuccess(None none)
          {
            try
            {
              callback.onSuccess(none);
            }
            finally
            {
              clientShutdown();
            }
          }

          @Override
          public void onError(Throwable e)
          {
            try
            {
              callback.onError(e);
            }
            finally
            {
              clientShutdown();
            }
          }
        });
      }
      else
      {
        callback.onError(new IllegalStateException("shutdown has already been requested."));
      }
    }
  }

  static class SwitchableClient implements TransportClient
  {
    private final TransportClient _legacyClient;
    private final TransportClient _streamClient;

    SwitchableClient(HttpNettyClient legacyClient, HttpNettyStreamClient streamClient)
    {
      _legacyClient = legacyClient;
      _streamClient = streamClient;
    }

    @Override
    public void restRequest(RestRequest request,
                            RequestContext requestContext,
                            Map<String, String> wireAttrs,
                            TransportCallback<RestResponse> callback)
    {
      _legacyClient.restRequest(request, requestContext, wireAttrs, callback);
    }

    @Override
    public void streamRequest(StreamRequest request,
                       RequestContext requestContext,
                       Map<String, String> wireAttrs,
                       TransportCallback<StreamResponse> callback)
    {
      _streamClient.streamRequest(request, requestContext, wireAttrs, callback);
    }

    @Override
    public void shutdown(final Callback<None> callback)
    {
      Callback<None> twiceCallback = new Callback<None>()
      {
        boolean _invoked = false;
        Throwable _error = null;

        @Override
        public void onError(Throwable e)
        {
          boolean invokeOriginalCallback = false;
          synchronized (this)
          {
            if (_invoked)
            {
              invokeOriginalCallback = true;
            }
            else
            {
              _invoked = true;
              _error = e;
            }
          }

          if (invokeOriginalCallback)
          {
            callback.onError(e);
          }
        }

        @Override
        public void onSuccess(None result)
        {
          boolean invokeOriginalCallback = false;
          synchronized (this)
          {
            if (_invoked)
            {
              invokeOriginalCallback = true;
            }
            else
            {
              _invoked = true;
            }
          }

          if (invokeOriginalCallback)
          {
            if (_error != null)
            {
              callback.onError(_error);
            }
            else
            {
              callback.onSuccess(result);
            }
          }
        }
      };

      _legacyClient.shutdown(twiceCallback);
      _streamClient.shutdown(twiceCallback);
    }


    long getRequestTimeout()
    {
      return ((HttpNettyStreamClient)_streamClient).getRequestTimeout();
    }

    long getShutdownTimeout()
    {
      return ((HttpNettyStreamClient)_streamClient).getShutdownTimeout();
    }

    long getMaxResponseSize()
    {
      return ((HttpNettyStreamClient)_streamClient).getMaxResponseSize();
    }
  }
}
