package com.linkedin.r2.message.streaming;

import com.linkedin.common.callback.Callback;
import com.linkedin.data.ByteString;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * This is a convenience Reader to assemble the full entity of a stream message.
 *
 * @author Zhenkai Zhu
 */
public final class FullEntityReader implements Reader
{
  private final NoCopyByteArrayOutputStream _outputStream = new NoCopyByteArrayOutputStream();
  private final Callback<ByteString> _callback;

  private ReadHandle _rh;

  /**
   * @param callback the callback to be invoked when the reader finishes assembling the full entity
   */
  public FullEntityReader(Callback<ByteString> callback)
  {
    _callback = callback;
  }

  public void onInit(ReadHandle rh)
  {
    _rh = rh;
    _rh.request(Integer.MAX_VALUE);
  }

  public void onDataAvailable(ByteString data)
  {
    try
    {
      data.write(_outputStream);
      _rh.request(1);
    }
    catch (Exception ex)
    {
      _callback.onError(ex);
      throw new RuntimeException(ex);
    }
  }

  public void onDone()
  {
    ByteString entity;

    try
    {
      entity = ByteString.read(_outputStream.toInputStream(), _outputStream.size());
    }
    catch (IOException ex)
    {
      _callback.onError(ex);
      return;
    }

    _callback.onSuccess(entity);
  }

  public void onError(Throwable ex)
  {
    _callback.onError(ex);
  }

  private static class NoCopyByteArrayOutputStream extends ByteArrayOutputStream
  {
    public synchronized InputStream toInputStream()
    {
      return new ByteArrayInputStream(super.buf, 0, super.count);
    }
  }
}
