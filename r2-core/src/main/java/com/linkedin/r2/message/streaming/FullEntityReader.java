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
  private final ByteString.Builder _builder;
  private final Callback<ByteString> _callback;

  private ReadHandle _rh;

  /**
   * @param callback the callback to be invoked when the reader finishes assembling the full entity
   */
  public FullEntityReader(Callback<ByteString> callback)
  {
    _callback = callback;
    _builder = new ByteString.Builder();
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
      _builder.append(data);
      _rh.request(1);
    }
    catch (Exception ex)
    {
      _rh.cancel();
      _callback.onError(ex);
    }
  }

  public void onDone()
  {
    ByteString entity = _builder.build();
    _callback.onSuccess(entity);
  }

  public void onError(Throwable ex)
  {
    _callback.onError(ex);
  }
}
