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


import org.jboss.netty.channel.ChannelDownstreamHandler;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelUpstreamHandler;

import java.util.concurrent.ScheduledExecutorService;


/**
* @author Steven Ihde
* @version $Revision: $
*/
class RAPClientCodec implements ChannelUpstreamHandler, ChannelDownstreamHandler
{
  private final RAPRequestEncoder _encoder = new RAPRequestEncoder();
  private final RAPResponseDecoder _decoder;

  RAPClientCodec(ScheduledExecutorService scheduler, int requestTimeout, int maxContentLength)
  {
    _decoder = new RAPResponseDecoder(scheduler, requestTimeout, maxContentLength);
  }

  @Override
  public void handleDownstream(ChannelHandlerContext ctx, ChannelEvent e) throws Exception
  {
    _encoder.handleDownstream(ctx, e);
  }

  @Override
  public void handleUpstream(ChannelHandlerContext ctx, ChannelEvent e) throws Exception
  {
    _decoder.handleUpstream(ctx, e);
  }

}
