package com.linkedin.r2.streaming.sample;

import com.linkedin.common.callback.Callback;
import com.linkedin.r2.message.RequestContext;
import com.linkedin.r2.message.rest.RestRequest;
import com.linkedin.r2.message.rest.RestRequestBuilder;
import com.linkedin.r2.message.rest.RestResponse;
import com.linkedin.r2.transport.common.Client;
import com.linkedin.r2.transport.common.RestRequestHandler;

import java.net.URI;

/**
 * A simple proxy that rewrites URI for request to downstream and relays response back to upstream.
 * Nevertheless, back pressure is still achieved.
 *
 * @author Zhenkai Zhu
 */
public class UriRewriteProxy implements RestRequestHandler
{
  final private Client _client;
  final private UriRewriter _uriRewriter;

  public UriRewriteProxy(Client client, UriRewriter uriRewriter)
  {
    _client = client;
    _uriRewriter = uriRewriter;
  }

  @Override
  public void handleRequest(RestRequest request, RequestContext requestContext, final Callback<RestResponse> callback)
  {
    URI newUri = _uriRewriter.rewrite(request.getURI());
    RestRequestBuilder builder = request.builder();
    builder.setURI(newUri);
    RestRequest newRequest = builder.build(request.getEntityStream());

    _client.restRequest(newRequest, requestContext, callback);
  }

  public interface UriRewriter
  {
    /**
     * Returns a new URI based on the input uri
     * e.g. d2://company/1000 -> http://192.168.0.1/company/1000
     * @param uri input uri
     * @return the rewritten uri
     */
    URI rewrite(URI uri);
  }
}
