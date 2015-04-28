package com.linkedin.r2.message.rest;

import com.linkedin.data.ByteString;
import com.linkedin.util.ArgumentUtil;

import java.net.URI;

/**
 * @author Zhenkai Zhu
 */
public final class RestRequestBuilder extends BaseRequestBuilder<RestRequestBuilder> implements RestMessageBuilder<RestRequestBuilder>
{
  private ByteString _entity = ByteString.empty();

  public RestRequestBuilder(URI uri)
  {
    super(uri);
  }

  public RestRequestBuilder(RestRequest request)
  {
    super(request);
    _entity = request.getEntity();
  }

  public RestRequestBuilder(StreamRequest request)
  {
    super(request);
  }

  public RestRequestBuilder setEntity(ByteString entity)
  {
    ArgumentUtil.notNull(entity, "entity");

    _entity = entity;
    return this;
  }

  public RestRequestBuilder setEntity(byte[] entity)
  {
    ArgumentUtil.notNull(entity, "entity");

    _entity = ByteString.copy(entity);
    return this;
  }

  public ByteString getEntity()
  {
    return _entity;
  }

  public RestRequest build()
  {
    return new RestRequestImpl(_entity, getHeaders(), getCookies(), getURI(), getMethod());
  }

  public RestRequest buildCanonical()
  {
    return new RestRequestImpl(_entity, getCanonicalHeaders(), getCanonicalCookies(), getURI(), getMethod());
  }
}
