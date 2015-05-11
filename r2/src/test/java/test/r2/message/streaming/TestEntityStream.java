package test.r2.message.streaming;

import com.linkedin.data.ByteString;
import com.linkedin.r2.message.streaming.EntityStream;
import com.linkedin.r2.message.streaming.EntityStreams;
import com.linkedin.r2.message.streaming.Observer;
import com.linkedin.r2.message.streaming.ReadHandle;
import com.linkedin.r2.message.streaming.Reader;
import com.linkedin.r2.message.streaming.WriteHandle;
import com.linkedin.r2.message.streaming.Writer;
import junit.framework.Assert;
import org.testng.annotations.Test;

/**
 * @author Zhenkai Zhu
 */
public class TestEntityStream
{

  @Test
  public void testEntityStream() throws Exception
  {
    TestWriter writer = new TestWriter();
    ControlReader reader = new ControlReader();
    TestObserver ob1 = new TestObserver();
    TestObserver ob2 = new TestObserver();
    EntityStream stream = EntityStreams.newEntityStream(writer);
    stream.addObserver(ob1);
    stream.addObserver(ob2);

    // write is not possible without a reader
    Assert.assertEquals(writer.getWritePossibleCount(), 0);

    stream.setReader(reader);
    // write is not possible before reader reads
    Assert.assertEquals(writer.getWritePossibleCount(), 0);

    reader.read(1);
    // write become possible
    Assert.assertEquals(writer.getWritePossibleCount(), 1);
    Assert.assertEquals(writer.remaining(), 1);
    writer.write();
    Assert.assertEquals(writer.remaining(), 0);

    reader.read(10);
    // write again become possible
    Assert.assertEquals(writer.getWritePossibleCount(), 2);
    Assert.assertEquals(writer.remaining(), 10);
    while(writer.remaining() > 1)
    {
      writer.write();
    }

    Assert.assertEquals(writer.remaining(), 1);
    reader.read(10);
    // write hasn't become impossible when reader reads again, so onWritePossible should not have been invoked again
    Assert.assertEquals(writer.getWritePossibleCount(), 2);

    while(writer.remaining() > 0)
    {
      writer.write();
    }

    Assert.assertEquals(ob1.getChunkCount(), 21);
    Assert.assertEquals(ob2.getChunkCount(), 21);
    Assert.assertEquals(reader.getChunkCount(), 21);

    try
    {
      writer.write();
      Assert.fail("should fail with IllegalStateException");
    }
    catch (IllegalStateException ex)
    {
      // expected
    }
  }

  private static class TestWriter implements Writer
  {
    private WriteHandle _wh;
    private volatile int _count = 0;
    private volatile boolean _aborted = false;

    public int getWritePossibleCount()
    {
      return _count;
    }

    public int remaining()
    {
      return _wh.remaining();
    }

    public void write()
    {
      _wh.write(ByteString.empty());
    }

    public boolean isAborted()
    {
      return _aborted;
    }

    @Override
    public void onInit(WriteHandle wh)
    {
      _wh = wh;
    }

    @Override
    public void onWritePossible()
    {
      _count++;
    }

    @Override
    public void onAbort(Throwable ex)
    {
      _aborted = true;
    }

  }

  private static class TestObserver implements Observer
  {
    private volatile int _count = 0;

    public int getChunkCount()
    {
      return _count;
    }

    @Override
    public void onDataAvailable(ByteString data)
    {
      _count++;
    }

    @Override
    public void onDone()
    {

    }

    @Override
    public void onError(Throwable e)
    {

    }
  }

  private static class ControlReader extends TestObserver implements Reader
  {
    ReadHandle _rh;

    public void read(int n)
    {
      _rh.request(n);
    }

    @Override
    public void onInit(ReadHandle rh)
    {
      _rh = rh;
    }
  }

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
          byte[] bytes = new byte[(int)Math.min(4096, _total - _count)];
          _wh.write(ByteString.copy(bytes));
          _count += bytes.length;
        }
        if (_count >= _total )
        {
          _wh.done();
        }
      }

      @Override
      public void onAbort(Throwable ex)
      {
        // do nothing
      }
    };

    Reader dumbReader = new Reader()
    {
      ReadHandle _rh;
      @Override
      public void onInit(ReadHandle rh)
      {
        _rh = rh;
        _rh.request(1);
      }

      @Override
      public void onDataAvailable(ByteString data)
      {
        _rh.request(1);
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
