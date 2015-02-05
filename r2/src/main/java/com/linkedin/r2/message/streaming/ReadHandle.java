package com.linkedin.r2.message.streaming;

/**
 * This is the handle to read data from an EntityStream {@link com.linkedin.r2.message.streaming.EntityStream}
 *
 * @author Zhenkai Zhu
 */
public interface ReadHandle
{
  /**
   * This method signals the writer of the EntityStream that it can write more data. This method does not block.
   *
   * @param chunkNum the additional number of chunks that the writer is permitted to write
   */
  void read(final int chunkNum);
}
