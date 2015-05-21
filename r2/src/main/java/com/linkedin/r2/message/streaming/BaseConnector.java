package com.linkedin.r2.message.streaming;

import com.linkedin.data.ByteString;

/**
 * @author Zhenkai Zhu
 */
public class BaseConnector implements Reader, Writer
{
  private WriteHandle _wh;
  private ReadHandle _rh;
  private int _outstanding;

  public BaseConnector()
  {
    _outstanding = 0;
  }

  @Override
  public void onInit(ReadHandle rh)
  {
    _rh = wrapReadHandle(rh);
  }

  @Override
  public void onInit(final WriteHandle wh)
  {
    _wh = wrapWriteHandle(wh);
  }


  @Override
  public void onDataAvailable(ByteString data)
  {
    _outstanding--;
    _wh.write(data);
    int diff = _wh.remaining() - _outstanding;
    if (diff > 0)
    {
      _rh.request(diff);
      _outstanding += diff;
    }
  }

  @Override
  public void onDone()
  {
    _wh.done();
  }

  @Override
  public void onError(Throwable e)
  {
    _wh.error(e);
  }

  @Override
  public void onWritePossible()
  {
    _outstanding = _wh.remaining();
    _rh.request(_outstanding);
  }

  @Override
  public void onAbort(Throwable e)
  {
  }

  protected WriteHandle wrapWriteHandle(WriteHandle wh)
  {
    return wh;
  }

  protected ReadHandle wrapReadHandle(ReadHandle rh)
  {
    return rh;
  }

  protected WriteHandle getWriteHandle()
  {
    return _wh;
  }

  protected ReadHandle getReadHandle()
  {
    return _rh;
  }
}