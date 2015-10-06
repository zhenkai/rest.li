package com.linkedin.r2.filter.compression.streaming;

import com.linkedin.data.ByteString;
import com.linkedin.r2.message.entitystream.WriteHandle;
import com.linkedin.r2.message.entitystream.Writer;
import java.util.Queue;


/**
 * @author Ang Xu
 */
public class ByteStringsWriter implements Writer
{
  private final Queue<ByteString> _contents;
  private WriteHandle _wh;

  public ByteStringsWriter(Queue<ByteString> contents)
  {
    _contents = contents;
  }

  @Override
  public void onInit(WriteHandle wh)
  {
    _wh = wh;
  }

  @Override
  public void onWritePossible()
  {
    while (_wh.remaining() > 0)
    {
      if (_contents.isEmpty())
      {
        _wh.done();
        return;
      }
      else
      {
        _wh.write(_contents.poll());
      }
    }
  }

  @Override
  public void onAbort(Throwable e)
  {

  }
}