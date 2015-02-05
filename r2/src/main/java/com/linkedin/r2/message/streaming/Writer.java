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
   * This is called when the Reader requested the writer to produce more data.
   *
   * @param chunkNum the additional chunks allowed to write
   */
  void onWritePossible(final int chunkNum);
}
