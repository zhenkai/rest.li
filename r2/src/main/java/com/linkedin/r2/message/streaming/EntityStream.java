package com.linkedin.r2.message.streaming;

/**
 * An object that represents the stream of request/response entity.
 *
 * Each entityStream can have one Writer, multiple Observers {@link com.linkedin.r2.message.streaming.Observer} and
 * exactly one Reader {@link com.linkedin.r2.message.streaming.Reader}. The data flow of a stream is reader
 * driven: that is, if reader doesn't read, there is no data flow.
 */
public interface EntityStream
{
  /**
   * Add observer to this stream.
   *
   * @param o the Observer
   * @throws java.lang.IllegalStateException if data had already start flowing
   */
  void addObserver(Observer o);

  /**
   * Set reader for this stream.
   *
   * @param r the Reader of this stream
   * @throws java.lang.IllegalStateException if there is already a reader
   */
  void setReader(Reader r);
}
