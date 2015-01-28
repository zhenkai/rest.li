package com.linkedin.r2.streaming.sample;

import com.linkedin.common.callback.Callback;
import com.linkedin.r2.message.RequestContext;
import com.linkedin.r2.message.rest.RestRequest;
import com.linkedin.r2.message.rest.RestResponse;
import com.linkedin.r2.transport.common.StreamClient;
import com.linkedin.r2.transport.common.StreamResponseHandler;

/**
 * A simple proxy that just relays request to downstream and response back to upstream.
 * Nevertheless, back pressure is still achieved.
 *
 * @author Zhenkai Zhu
 */
public class SimpleRelayProxy implements StreamResponseHandler
{
  final private StreamClient _client;

  public SimpleRelayProxy(StreamClient client)
  {
    _client = client;
  }

  @Override
  public void handleStreamRequest(RestRequest request, RequestContext requestContext, final Callback<RestResponse> callback)
  {
    /**
     * RestRequest is created by async servlet, and a Writer would be provided during construction. This RequestRequest
     * would be passed through server filter chain, to this method, and then be passed down to the client filter chain,
     * and finally reach the netty client code. Netty client code would then set a Reader to the entity stream of this
     * Request. So when there is capacity to sent to downstream, netty client (Reader) would signal async servlet (Writer)
     * to asynchrously write certain amount data (e.g. 1024K) when the servlet has data read from upstream. If netty
     * client has no capacity to send, the servlet won't read from upstream.
     *
     * Similarly, RestResponse is created by the netty client, and has a Writer provided. The RestResponse would be passed
     * through client filter chain and server filter chain and then passed to async servlet, which in turn would set
     * a Reader to the entity stream. So when there is capacity to sent response back to upstream, async servlet (Reader)
     * would signal netty client (Writer) to write certain amount data (e.g. 1024K) when netty client has data read
     * from downstream. If the servlet has no capacity to send, the netty client won't read from downstream.
     */
    _client.streamRestRequest(request, requestContext, callback);
  }
}
