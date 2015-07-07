package com.linkedin.r2.filter.compression.streaming;

import com.linkedin.data.ByteString;
import com.linkedin.r2.message.streaming.ReadHandle;
import com.linkedin.r2.message.streaming.Reader;


/**
 * A reader to cancel an unstarted {@link com.linkedin.r2.message.streaming.EntityStream} and
 * abort its underlying {@link com.linkedin.r2.message.streaming.Writer}.
 *
 * @author Ang Xu
 */
public final class CancelingReader implements Reader
{
  @Override
  public void onInit(ReadHandle rh)
  {
    rh.cancel();
  }

  @Override
  public void onDataAvailable(ByteString data)
  {
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
