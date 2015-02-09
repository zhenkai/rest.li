package com.linkedin.r2.message.streaming;

/**
 * Writer is the producer of data for an EntityStream {@link com.linkedin.r2.message.streaming.EntityStream}
 *
 * @author Zhenkai Zhu
 */
public interface Writer
{
  /**
   * This is called when a Reader is set for the EntityStream.
   *
   * @param wh the handle to write data to the EntityStream.
   * @param chunkSize the desired chunk size for data
   */
  void onInit(final WriteHandle wh, final int chunkSize);

  /**
   * Invoked when it it possible to write data.
   *
   * This method will be invoked the first time as soon as data can be written to the WriteHandle.
   * Subsequent invocations will only occur if a call to WriteHandle.isWritable() has returned false
   * and it has since become possible to write data.
   *
   * This invocation pattern is identical to WriteListener.onWritePossible in Servlet API 3.1 and
   * maybe Netty 4 ChannelInboundHandler.channelWritabilityChanged; the latter didn't say exactly as above,
   * but the use pattern is the same (used together with Channel.isWritable).
   */
  void onWritePossible();
}
