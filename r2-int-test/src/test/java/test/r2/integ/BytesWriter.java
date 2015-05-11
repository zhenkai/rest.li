package test.r2.integ;

/**
 * @author Zhenkai Zhu
 */

import com.linkedin.data.ByteString;
import com.linkedin.r2.message.streaming.WriteHandle;
import com.linkedin.r2.message.streaming.Writer;
import com.linkedin.r2.sample.echo.ThrowingEchoService;

import java.util.Arrays;

/** package private */ class BytesWriter implements Writer
{
  private final long _total;
  private final byte _fill;
  private long _written;
  private WriteHandle _wh;
  private boolean _error = false;

  BytesWriter(long total, byte fill)
  {
    _total = total;
    _fill = fill;
    _written = 0;
  }

  @Override
  public void onInit(WriteHandle wh)
  {
    _wh = wh;
  }

  @Override
  public void onWritePossible()
  {

    while(_wh.remaining() >  0 && _written < _total && !_error)
    {
      int bytesNum = (int)Math.min(4096, _total - _written);
      _wh.write(generate(bytesNum));
      _written += bytesNum;
      afterWrite(_wh, _written);
    }

    if (_written == _total && !_error)
    {
      _wh.done();
      onFinish();
    }
  }

  @Override
  public void onAbort(Throwable ex)
  {
    // do nothing
  }

  protected void onFinish()
  {
    // nothing
  }

  protected void afterWrite(WriteHandle wh, long written)
  {
    // nothing
  }

  protected void markError()
  {
    _error = true;
  }

  private ByteString generate(int size)
  {
    byte[] result = new byte[size];
    Arrays.fill(result, _fill);
    return ByteString.copy(result);
  }
}
