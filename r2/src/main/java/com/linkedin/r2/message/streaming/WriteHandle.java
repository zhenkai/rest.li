package com.linkedin.r2.message.streaming;

import com.linkedin.data.ByteString;

/**
 * This is the handle to write data to an EntityStream.
 * This is not thread-safe.
 *
 * @author Zhenkai Zhu
 */
public interface WriteHandle
{
  /**
   * This writes data into the EntityStream.
   *
   * @param data the data chunk to be written
   * @throws java.lang.IllegalStateException if remaining capacity is 0, or done() or error() has been called
   */
  void write(final ByteString data);

  /**
   * Signals that Writer has finished writing.
   *
   */
  void done();

  /**
   * Signals that the Writer has encountered an error.
   *
   * @param throwable the cause of the error.
   */
  void error(final Throwable throwable);

  /**
   * Returns the remaining capacity in number of data chunks
   *
   * @return the remaining capacity in number of data chunks
   */
  int remaining();
}
