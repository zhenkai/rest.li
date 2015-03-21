package test.r2.integ;

/**
 * @author Zhenkai Zhu
 */

import com.linkedin.data.ByteString;
import com.linkedin.r2.message.streaming.WriteHandle;
import com.linkedin.r2.message.streaming.Writer;

import java.util.Arrays;

/** package private */ class BytesWriter implements Writer
{
  private final int _total;
  private final byte _fill;
  private int _written;
  private WriteHandle _wh;

  BytesWriter(int total, byte fill)
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

    while(_wh.remainingCapacity() >  0 && _written < _total)
    {
      int bytesNum = Math.min(_wh.remainingCapacity(), _total - _written);
      _wh.write(generate(bytesNum));
      _written += bytesNum;
    }

    if (_written == _total)
    {
      _wh.done();
    }
  }

  private ByteString generate(int size)
  {
    byte[] result = new byte[size];
    Arrays.fill(result, _fill);
    return ByteString.copy(result);
  }
}
