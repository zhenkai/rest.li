package com.linkedin.r2.message.streaming;

import com.linkedin.data.ByteString;

import java.io.IOException;

/**
 * @author Zhenkai Zhu
 */
public class DrainReader implements Reader
{
  private ReadHandle _rh;

  public void onInit(ReadHandle rh)
  {
    _rh = rh;
    _rh.read(Integer.MAX_VALUE);
  }

  public void onDataAvailable(ByteString data)
  {
    _rh.read(data.length());
  }

  public void onDone()
  {
  }

  public void onError(Throwable ex)
  {
  }

}
