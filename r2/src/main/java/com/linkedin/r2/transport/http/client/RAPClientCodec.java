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


import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import java.util.concurrent.ScheduledExecutorService;


/**
 * @author Steven Ihde
 * @author Ang Xu
 * @version $Revision: $
 */
class RAPClientCodec extends ChannelDuplexHandler
{
  private final RAPRequestEncoder _encoder;
  private final RAPResponseDecoder _decoder;

  RAPClientCodec(ScheduledExecutorService scheduler, long requestTimeout, int maxContentLength)
  {
    _encoder = new RAPRequestEncoder();
    _decoder = new RAPResponseDecoder(scheduler, requestTimeout, maxContentLength);
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception
  {
    _decoder.channelRead(ctx, msg);
  }

  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception
  {
    _encoder.write(ctx, msg, promise);
  }
}
