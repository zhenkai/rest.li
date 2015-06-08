package com.linkedin.r2.transport.http.client;

import com.linkedin.r2.message.rest.RestRequest;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.HttpRequest;

/**
 * This encoder encodes RestRequest to Netty's HttpRequest.
 *
 * @auther Zhenkai Zhu
 */

class RAPFullRequestEncoder extends ChannelOutboundHandlerAdapter
{
  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception
  {
    if (msg instanceof RestRequest)
    {
      RestRequest request = (RestRequest) msg;
      HttpRequest nettyRequest = NettyRequestAdapter.toNettyRequest(request);
      ctx.write(nettyRequest, promise);
    }
    else
    {
      ctx.write(msg, promise);
    }
  }

  @Override
  public void flush(ChannelHandlerContext ctx)
  {
    ctx.flush();
  }
}
