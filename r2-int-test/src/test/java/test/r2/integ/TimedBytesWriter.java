package test.r2.integ;

import com.linkedin.r2.message.streaming.WriteHandle;

/**
 * @author Zhenkai Zhu
 */
class TimedBytesWriter extends BytesWriter
{
  private long _startTime;
  private long _stopTime;

  TimedBytesWriter(int total, byte fill)
  {
    super(total, fill);
  }

  @Override
  public void onInit(WriteHandle wh)
  {
    _startTime = System.currentTimeMillis();
    super.onInit(wh);
  }

  @Override
  public void onWritePossible()
  {
    super.onWritePossible();
  }

  @Override
  protected void onFinish()
  {
    super.onFinish();
    _stopTime = System.currentTimeMillis();
  }

  public long getStartTime()
  {
    return _startTime;
  }

  public long getStopTime()
  {
    return _stopTime;
  }
}
