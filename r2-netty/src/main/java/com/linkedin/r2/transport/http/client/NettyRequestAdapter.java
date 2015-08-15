package com.linkedin.r2.transport.http.client;

import com.linkedin.r2.message.rest.Request;
import com.linkedin.r2.message.rest.RestRequest;
import com.linkedin.r2.message.rest.StreamRequest;
import com.linkedin.r2.transport.http.common.HttpConstants;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpVersion;

import java.net.URL;
import java.util.Map;

/**
 * @auther Zhenkai Zhu
 */

/* package private */ class NettyRequestAdapter
{
  private NettyRequestAdapter() {}

  /**
   * Adapts a Request to Netty's HttpRequest
   * @param request R2 request
   * @return Adapted HttpRequest.
   */
  static HttpRequest toNettyRequest(Request request) throws Exception
  {
    HttpMethod nettyMethod = HttpMethod.valueOf(request.getMethod());
    URL url = new URL(request.getURI().toString());
    String path = url.getFile();
    // RFC 2616, section 5.1.2:
    //   Note that the absolute path cannot be empty; if none is present in the original URI,
    //   it MUST be given as "/" (the server root).
    if (path.isEmpty())
    {
      path = "/";
    }

    HttpRequest nettyRequest;
    if (request instanceof StreamRequest)
    {
      nettyRequest = new DefaultHttpRequest(HttpVersion.HTTP_1_1, nettyMethod, path);
      nettyRequest.headers().set(HttpHeaders.Names.TRANSFER_ENCODING, HttpHeaders.Values.CHUNKED);
    }
    else if (request instanceof RestRequest)
    {
      RestRequest restRequest = (RestRequest)request;
      ByteBuf content = Unpooled.wrappedBuffer(restRequest.getEntity().asByteBuffer());
      nettyRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, nettyMethod, path, content);
      nettyRequest.headers().set(HttpConstants.CONTENT_LENGTH, restRequest.getEntity().length());
    }
    else
    {
      throw new IllegalArgumentException("Unknown type of request: " + request);
    }

    for (Map.Entry<String, String> entry : request.getHeaders().entrySet())
    {
      nettyRequest.headers().set(entry.getKey(), entry.getValue());
    }
    nettyRequest.headers().set("branch", "stream");
    nettyRequest.headers().set(HttpHeaders.Names.HOST, url.getAuthority());
    nettyRequest.headers().set(HttpConstants.REQUEST_COOKIE_HEADER_NAME, request.getCookies());

    return nettyRequest;
  }
}
