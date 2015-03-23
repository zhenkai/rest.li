package test.r2.integ;

import com.linkedin.common.callback.Callback;
import com.linkedin.data.ByteString;
import com.linkedin.r2.message.rest.RestException;
import com.linkedin.r2.message.rest.RestResponse;
import com.linkedin.r2.message.rest.RestStatus;
import com.linkedin.r2.message.rest.StreamResponse;
import com.linkedin.r2.message.streaming.ReadHandle;
import com.linkedin.r2.message.streaming.Reader;

/**
 * @author Zhenkai Zhu
 */
class BytesReader implements Reader
{
  private final byte _b;
  private final Callback<StreamResponse> _callback;
  private final int _status;
  private int _length;
  private boolean _bytesCorrect;
  private ReadHandle _rh;

  BytesReader(byte b, Callback<StreamResponse> callback, int responseStatus)
  {
    _b = b;
    _callback = callback;
    _status = responseStatus;
    _bytesCorrect = true;
    _length = 0;
  }

  @Override
  public void onInit(ReadHandle rh)
  {
    _rh = rh;
    _rh.read(16 * 1024);
  }

  @Override
  public void onDataAvailable(ByteString data)
  {
    _length += data.length();
    byte [] bytes = data.copyBytes();
    for (byte b : bytes)
    {
      if (b != _b)
      {
        _bytesCorrect = false;
      }
    }
    requestMore(_rh, data.length());
  }

  @Override
  public void onDone()
  {
    RestResponse response = RestStatus.responseForStatus(_status, "");
    _callback.onSuccess(response);
  }

  @Override
  public void onError(Throwable e)
  {
    RestException restException = new RestException(RestStatus.responseForError(500, e));
    _callback.onError(restException);
  }

  public int getTotalBytes()
  {
    return _length;
  }

  public boolean allBytesCorrect()
  {
    return _bytesCorrect;
  }

  protected void requestMore(ReadHandle rh, int processedDataLen)
  {
    rh.read(processedDataLen);
  }
}
