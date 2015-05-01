package com.linkedin.r2.message.streaming;

import com.linkedin.common.callback.Callback;
import com.linkedin.data.ByteString;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * @author Zhenkai Zhu
 */
public final class FullEntityReader implements Reader
{
  private final NoCopyByteArrayOutputStream _outputStream = new NoCopyByteArrayOutputStream();
  private final Callback<ByteString> _callback;

  private ReadHandle _rh;

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
    } catch (Exception ex)
    {
      _callback.onError(ex);

    }
  }

  public void onDone()
  {
    ByteString entity = ByteString.empty();

    // commons-io 2.5 ByteArrayOutputStream has toInputStream() method; the returned stream is backed
    // by buffers of this stream, avoiding memory allocation and copy
    // but commons-io 2.5 is still SNAPSHOT version in their website, so we hacked our own

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
