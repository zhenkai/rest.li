package com.linkedin.r2.message.streaming;

import com.linkedin.data.ByteString;

/**
 * @author Zhenkai Zhu
 */
public class DrainReader implements Reader
{
  private ReadHandle _rh;

  public void onInit(ReadHandle rh)
  {
    _rh = rh;
    _rh.request(Integer.MAX_VALUE);
  }

  public void onDataAvailable(ByteString data)
  {
    _rh.request(1);
  }

  public void onDone()
  {
  }

  public void onError(Throwable ex)
  {
  }

}
