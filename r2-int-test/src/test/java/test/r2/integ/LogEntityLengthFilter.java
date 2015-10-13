package test.r2.integ;

import com.linkedin.data.ByteString;
import com.linkedin.r2.filter.NextFilter;
import com.linkedin.r2.filter.message.stream.StreamRequestFilter;
import com.linkedin.r2.filter.message.stream.StreamResponseFilter;
import com.linkedin.r2.message.RequestContext;
import com.linkedin.r2.message.stream.StreamRequest;
import com.linkedin.r2.message.stream.StreamResponse;
import com.linkedin.r2.message.stream.entitystream.Observer;

import java.util.Map;

/**
 * @author Zhenkai Zhu
 */
public class LogEntityLengthFilter implements StreamRequestFilter, StreamResponseFilter
{
  private volatile int _reqEntityLen = 0;
  private volatile int _resEntityLen = 0;

  @Override
  public void onRequest(StreamRequest req,
                 RequestContext requestContext,
                 Map<String, String> wireAttrs,
                 NextFilter<StreamRequest, StreamResponse> nextFilter)
  {
    req.getEntityStream().addObserver(new LengthObserver(true));
    nextFilter.onRequest(req, requestContext, wireAttrs);
  }

  @Override
  public void onResponse(StreamResponse res,
                  RequestContext requestContext,
                  Map<String, String> wireAttrs,
                  NextFilter<StreamRequest, StreamResponse> nextFilter)
  {
    res.getEntityStream().addObserver(new LengthObserver(false));
    nextFilter.onResponse(res, requestContext, wireAttrs);
  }

  @Override
  public void onError(Throwable ex,
               RequestContext requestContext,
               Map<String, String> wireAttrs,
               NextFilter<StreamRequest, StreamResponse> nextFilter)
  {
    nextFilter.onError(ex, requestContext, wireAttrs);
  }

  public int getRequestEntityLength()
  {
    return _reqEntityLen;
  }

  public int getResponseEntityLength()
  {
    return _resEntityLen;
  }

  private class LengthObserver implements Observer
  {
    private final boolean _isRequest;
    private int _length = 0;

    LengthObserver(boolean isRequest)
    {
      _isRequest = isRequest;
    }

    @Override
    public void onDataAvailable(ByteString data)
    {
      _length += data.length();
    }

    @Override
    public void onDone()
    {
      if (_isRequest)
      {
        _reqEntityLen = _length;
      }
      else
      {
        _resEntityLen = _length;
      }
    }

    @Override
    public void onError(final Throwable e)
    {
      _length = -1;
      onDone();
    }
  }
}
