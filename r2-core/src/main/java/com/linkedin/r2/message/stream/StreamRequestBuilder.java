package com.linkedin.r2.message.stream;

import com.linkedin.r2.message.BaseRequestBuilder;
import com.linkedin.r2.message.Request;
import com.linkedin.r2.message.stream.entitystream.EntityStream;

import java.net.URI;

/**
 * @author Zhenkai Zhu
 */
public final class StreamRequestBuilder extends BaseRequestBuilder<StreamRequestBuilder>
    implements StreamMessageBuilder<StreamRequestBuilder>
{
  /**
   * Constructs a new builder using the given uri.
   *
   * @param uri the URI for the resource involved in the request
   */
  public StreamRequestBuilder(URI uri)
  {
    super(uri);
  }

  /**
   * Copies the values from the supplied request. Changes to this builder will not be reflected
   * in the original message.
   *
   * @param request the request to copy
   */
  public StreamRequestBuilder(Request request)
  {
    super(request);
  }

  @Override
  public StreamRequest build(EntityStream entityStream)
  {
    return new StreamRequestImpl(entityStream, getHeaders(), getCookies(), getURI(), getMethod());
  }

  @Override
  public StreamRequest buildCanonical(EntityStream entityStream)
  {
    return new StreamRequestImpl(entityStream, getCanonicalHeaders(), getCanonicalCookies(), getURI().normalize(), getMethod());
  }
}
