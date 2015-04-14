package com.linkedin.r2.message.streaming;

import com.linkedin.data.ByteString;
import com.linkedin.util.ArgumentUtil;

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
    ArgumentUtil.notNull(content, "content");
    _content = content;
    _offset = 0;
  }

  public void onInit(WriteHandle wh)
  {
    _wh = wh;
  }

  public void onWritePossible()
  {
    while(_wh.remaining() > 0 && _offset <= _content.length())
    {
      if (_offset == _content.length())
      {
        _wh.done();
        break;
      }
      int bytesToWrite = Math.min(_wh.remaining(), _content.length() - _offset);
      _wh.write(_content.slice(_offset, bytesToWrite));
      _offset += bytesToWrite;
    }
  }
}
