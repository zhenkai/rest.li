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
package test.r2.perf.client;


import com.linkedin.common.callback.Callbacks;
import com.linkedin.common.util.None;
import com.linkedin.r2.filter.CompressionConfig;
import com.linkedin.r2.filter.Filter;
import com.linkedin.r2.filter.FilterChains;
import com.linkedin.r2.filter.compression.ClientCompressionFilter;
import com.linkedin.r2.filter.compression.EncodingType;
import com.linkedin.r2.message.rest.RestRequest;
import com.linkedin.r2.message.rest.StreamRequest;
import com.linkedin.r2.transport.common.Client;
import com.linkedin.r2.transport.common.TransportClientFactory;
import com.linkedin.r2.transport.common.bridge.client.TransportClient;
import com.linkedin.r2.transport.common.bridge.client.TransportClientAdapter;
import com.linkedin.r2.transport.http.client.AbstractJmxManager;
import com.linkedin.r2.transport.http.client.HttpClientFactory;

import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.Executors;

import com.linkedin.r2.util.NamedThreadFactory;
import io.netty.channel.nio.NioEventLoopGroup;
import test.r2.perf.Generator;


/**
 * @author Chris Pettitt
 * @version $Revision$
 */
public class PerfClients
{

  public static PerfClient httpRest(URI uri,
                                    int numThreads,
                                    int numMsgs,
                                    int msgSize,
                                    int qps,
                                    int warmUpMs,
                                    String acceptEncoding)
  {
    final TransportClientFactory factory = createFactory(acceptEncoding);
    final TransportClient transportClient = factory.getClient(Collections.<String, String>emptyMap());
    final Client client = new TransportClientAdapter(transportClient);
    final Generator<RestRequest> reqGen = new RestRequestGenerator(uri, numMsgs, msgSize);
    final Generator<RestRequest> warmUpReqGen = new RestRequestGenerator(uri, Integer.MAX_VALUE, msgSize);
    final ClientRunnableFactory crf = new RestClientRunnableFactory(client, reqGen, warmUpReqGen, qps);

    return new FactoryClient(crf, numThreads, warmUpMs, factory);
  }

  public static PerfClient httpStream(URI uri,
                                      int numThreads,
                                      int numMsgs,
                                      int msgSize,
                                      int qps,
                                      int warmUpMs,
                                      String acceptEncoding)
  {
    final TransportClientFactory factory = createFactory(acceptEncoding);
    final TransportClient transportClient = factory.getClient(Collections.<String, String>emptyMap());
    final Client client = new TransportClientAdapter(transportClient);
    final Generator<StreamRequest> reqGen = new StreamRequestGenerator(uri, numMsgs, msgSize);
    final Generator<StreamRequest> warmUpReqGen = new StreamRequestGenerator(uri, Integer.MAX_VALUE, msgSize);
    final ClientRunnableFactory crf = new StreamClientRunnableFactory(client, reqGen, warmUpReqGen, qps);

    return new FactoryClient(crf, numThreads, warmUpMs, factory);
  }

  private static TransportClientFactory createFactory(String acceptEncodings)
  {
    Filter compressionFilter =
        new ClientCompressionFilter("",
                                    new CompressionConfig(0),
                                    acceptEncodings,
                                    Arrays.asList(new String[]{"*"}),
                                    Executors.newCachedThreadPool());
    return new HttpClientFactory(FilterChains.create(compressionFilter),
        new NioEventLoopGroup(0 /* use default settings */, new NamedThreadFactory("R2 Nio Event Loop")),
        true,
        Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("R2 Netty Scheduler")),
        true,
        Executors.newFixedThreadPool(24),
        true);
  }

  private static class FactoryClient extends PerfClient
  {
    private final TransportClientFactory _factory;

    public FactoryClient(ClientRunnableFactory runnableFactory,
                         int numThreads, int warmUpMs,
                         TransportClientFactory factory)
    {
      super(runnableFactory, numThreads, warmUpMs);
      _factory = factory;
    }

    @Override
    public void shutdown()
    {
      super.shutdown();
      _factory.shutdown(Callbacks.<None>empty());
    }
  }
}
