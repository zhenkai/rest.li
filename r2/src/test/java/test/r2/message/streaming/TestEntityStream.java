package test.r2.message.streaming;

import com.linkedin.data.ByteString;
import com.linkedin.r2.message.streaming.EntityStreams;
import com.linkedin.r2.message.streaming.ReadHandle;
import com.linkedin.r2.message.streaming.Reader;
import com.linkedin.r2.message.streaming.WriteHandle;
import com.linkedin.r2.message.streaming.Writer;
import org.testng.annotations.Test;

/**
 * @author Zhenkai Zhu
 */
public class TestEntityStream
{


  @Test
  public void testNoStackOverflow() throws Exception
  {
    Writer dumbWriter = new Writer()
    {
      WriteHandle _wh;
      long _count = 0;
      final int _total = 1024 * 1024 * 1024;
      @Override
      public void onInit(WriteHandle wh)
      {
        _wh = wh;
      }

      @Override
      public void onWritePossible()
      {
        while(_wh.remaining() > 0 && _count < _total )
        {
          byte[] bytes = new byte[Math.min(4096, _wh.remaining())];
          _wh.write(ByteString.copy(bytes));
          _count += bytes.length;
        }
        if (_count >= _total )
        {
          _wh.done();
        }
      }
    };

    Reader dumbReader = new Reader()
    {
      ReadHandle _rh;
      @Override
      public void onInit(ReadHandle rh)
      {
        _rh = rh;
        _rh.read(4096);
      }

      @Override
      public void onDataAvailable(ByteString data)
      {
        _rh.read(data.length());
      }

      @Override
      public void onDone()
      {

      }

      @Override
      public void onError(Throwable e)
      {

      }
    };

    EntityStreams.newEntityStream(dumbWriter).setReader(dumbReader);
  }
}
