package com.linkedin.r2.message.streaming;

import com.linkedin.data.ByteString;

/**
 * A  writer that produce content based on the ByteString body
 */
public class ByteStringWriter implements Writer
{
  final ByteString _content;
  private int _offset;
  private WriteHandle _wh;

  public ByteStringWriter(ByteString content)
  {
    _content = content;
    _offset = 0;
  }

  public void onInit(WriteHandle wh)
  {
    _wh = wh;
  }

  public void onWritePossible()
  {
    while(_wh.remainingCapacity() > 0 && _offset < _content.length())
    {
      int bytesToWrite = Math.min(_wh.remainingCapacity(), _content.length() - _offset);
      _wh.write(_content.slice(_offset, bytesToWrite));
      _offset += bytesToWrite;
      if (_offset == _content.length())
      {
        _wh.done();
      }
    }
  }
}
