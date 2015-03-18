package com.linkedin.r2.message.rest;

import com.linkedin.data.ByteString;
import com.linkedin.r2.message.MessageBuilder;
import com.linkedin.r2.message.streaming.EntityStream;
import com.linkedin.r2.message.streaming.EntityStreams;
import com.linkedin.util.ArgumentUtil;

/**
 * @author Zhenkai Zhu
 */
public final class RestResponseBuilder extends BaseResponseBuilder<RestResponseBuilder> implements MessageBuilder<RestResponseBuilder>
{
  private ByteString _entity = ByteString.empty();

  public RestResponseBuilder() {}

  public RestResponseBuilder(RestResponse response)
  {
    super(response);
    _entity = response.getEntity();
  }

  public RestResponseBuilder(StreamResponse response)
  {
    super(response);
  }

  public RestResponseBuilder(StreamResponseBuilder builder)
  {
    this(builder.build(EntityStreams.emptyStream()));
  }

  public RestResponseBuilder setEntity(ByteString entity)
  {
    ArgumentUtil.notNull(entity, "entity");

    _entity = entity;
    return this;
  }

  public RestResponseBuilder setEntity(byte[] entity)
  {
    ArgumentUtil.notNull(entity, "entity");

    _entity = ByteString.copy(entity);
    return this;
  }

  public ByteString getEntity()
  {
    return _entity;
  }

  public RestResponse build()
  {
    return new RestResponseImpl(_entity, getHeaders(), getCookies(), getStatus());
  }

  public RestResponse buildCanonical()
  {
    return new RestResponseImpl(_entity, getCanonicalHeaders(), getCanonicalCookies(), getStatus());
  }

  @Override
  public StreamResponse build(EntityStream entityStream)
  {
    return new StreamResponseImpl(entityStream, getHeaders(), getCookies(), getStatus());
  }

  @Override
  public StreamResponse buildCanonical(EntityStream entityStream)
  {
    return new StreamResponseImpl(entityStream, getCanonicalHeaders(), getCanonicalCookies(), getStatus());
  }
}
