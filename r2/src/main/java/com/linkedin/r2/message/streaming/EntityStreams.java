package com.linkedin.r2.message.streaming;

/**
 * A factory style class to create new EntityStream {@link com.linkedin.r2.message.streaming.EntityStream}
 *
 * @author Zhenkai Zhu
 */
public final class EntityStreams
{
  /**
   * The method to create a new EntityStream with a writer for the stream
   *
   * @param writer the writer for the stream who would provide the data
   * @return an instance of EntityStream
   */
  public static EntityStream newEntityStream(Writer writer)
  {
    // TBD, just return a dummy impl now
    return new EntityStreamImpl(writer);
  }

  private static class EntityStreamImpl implements EntityStream
  {
    EntityStreamImpl(Writer writer)
    {
      // TBD
    }

    public void addObserver(Observer o)
    {
      // TBD
    }

    public void setReader(Reader r)
    {
      // TBD
    }
  }
}
