package com.linkedin.r2.message.rest;

import com.linkedin.data.ByteString;
import com.linkedin.r2.message.streaming.EntityStream;
import com.linkedin.r2.message.streaming.EntityStreams;
import com.linkedin.r2.message.streaming.WriteHandle;
import com.linkedin.r2.message.streaming.Writer;
import com.linkedin.util.ArgumentUtil;

import java.net.URI;
import java.util.List;
import java.util.Map;

/**
 * @author Zhenkai Zhu
 */
/* package private */ final class RestRequestImpl extends StreamRequestImpl implements RestRequest
{
  private final ByteString _entity;

  /* package private */ RestRequestImpl(ByteString entity, Map<String, String> headers,
                                        List<String> cookies, URI uri, String method)
  {
    super(EntityStreams.emptyStream(), headers, cookies, uri, method);
    ArgumentUtil.notNull(entity, "entity");
    _entity = entity;
  }

  @Override
  public ByteString getEntity()
  {
    return _entity;
  }

  @Override
  public EntityStream getEntityStream()
  {
    return EntityStreams.newEntityStream(new ByteStringWriter(_entity));
  }

  @Override
  public RestRequestBuilder builder()
  {
    return new RestRequestBuilder(this);
  }

  @Override
  public RestRequestBuilder requestBuilder()
  {
    return builder();
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o)
    {
      return true;
    }
    if (!(o instanceof RestRequestImpl))
    {
      return false;
    }
    if (!super.equals(o))
    {
      return false;
    }

    RestRequestImpl that = (RestRequestImpl) o;
    return _entity.equals(that._entity);
  }

  @Override
  public int hashCode()
  {
    int result = super.hashCode();
    result = result + 31 * _entity.hashCode();
    return result;
  }

  @Override
  public String toString()
  {
    StringBuilder builder = new StringBuilder();
    builder.append("RestRequest[headers=")
        .append(getHeaders())
        .append("cookies=")
        .append(getCookies())
        .append(",uri=")
        .append(getURI())
        .append(",method=")
        .append(getMethod())
        .append(",entityLength=")
        .append(_entity.length())
        .append("]");
    return builder.toString();
  }

  /**
   * A private writer that produce content based on the ByteString body
   */
  private static class ByteStringWriter implements Writer
  {
    final ByteString _content;
    private int _offset;
    private WriteHandle _wh;

    ByteStringWriter(ByteString content)
    {
      _content = content;
      _offset = 0;
    }

    public void onInit(WriteHandle wh)
    {
      _wh = wh;
    }

    public void onWritePossible()
    {
      while(_wh.remainingCapacity() > 0 && _offset < _content.length())
      {
        int bytesToWrite = Math.min(_wh.remainingCapacity(), _content.length() - _offset);
        _wh.write(_content.slice(_offset, bytesToWrite));
        _offset += bytesToWrite;
        if (_offset == _content.length())
        {
          _wh.done();
        }
      }
    }
  }
}
