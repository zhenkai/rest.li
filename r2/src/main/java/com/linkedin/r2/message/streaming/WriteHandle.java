package com.linkedin.r2.message.streaming;

import com.linkedin.data.ByteString;

/**
 * This is the handle to write data to an EntityStream.
 *
 * @author Zhenkai Zhu
 */
public interface WriteHandle
{
  /**
   * This writes data into the EntityStream. This method does not block.
   *
   * @param data the data to be written
   * @throws java.lang.IllegalArgumentException if the length of the data exceeds the total remaining capacity
   * permitted by the Reader
   */
  void write(ByteString data);

  /**
   * Signals that Writer has finished writing. This method does not block.
   */
  void done();

  /**
   * Signals that the Writer has encountered an error. This method does not block.
   *
   * @param throwable the cause of the error.
   */
  void error(Throwable throwable);
}
