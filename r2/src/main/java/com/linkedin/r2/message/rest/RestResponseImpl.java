package com.linkedin.r2.message.rest;

import com.linkedin.data.ByteString;
import com.linkedin.r2.message.streaming.ByteStringWriter;
import com.linkedin.r2.message.streaming.EntityStream;
import com.linkedin.r2.message.streaming.EntityStreams;
import com.linkedin.util.ArgumentUtil;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Zhenkai Zhu
 */
/* package private */ final class RestResponseImpl extends BaseResponse implements RestResponse
{
  private final ByteString _entity;

  /* package private */ RestResponseImpl(ByteString entity, Map<String, String> headers, List<String> cookies, int status)
  {
    super(headers, cookies, status);
    ArgumentUtil.notNull(entity, "entity");
    _entity = entity;
  }

  @Override
  public ByteString getEntity()
  {
    return _entity;
  }

  @Override
  public RestResponseBuilder builder()
  {
    return new RestResponseBuilder(this);
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o)
    {
      return true;
    }
    if (!(o instanceof RestResponseImpl))
    {
      return false;
    }
    if (!super.equals(o))
    {
      return false;
    }

    RestResponseImpl that = (RestResponseImpl) o;
    return _entity.equals(that._entity);
  }

  @Override
  public int hashCode()
  {
    int result = super.hashCode();
    result = 31 * result + _entity.hashCode();
    return result;
  }

  @Override
  public String toString()
  {
    StringBuilder builder = new StringBuilder();
    builder.append("RestResponse[headers=")
        .append(getHeaders())
        .append("cookies=")
        .append(getCookies())
        .append(",status=")
        .append(getStatus())
        .append(",entityLength=")
        .append(_entity.length())
        .append("]");
    return builder.toString();
  }
}
