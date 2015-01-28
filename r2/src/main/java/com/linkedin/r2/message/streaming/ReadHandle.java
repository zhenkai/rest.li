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
   * @param bytesNum the additional number of bytes that the writer is permitted to write
   */
  void read(final int bytesNum);
}
