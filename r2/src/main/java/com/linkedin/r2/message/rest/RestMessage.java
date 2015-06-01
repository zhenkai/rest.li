package com.linkedin.r2.message.rest;

import com.linkedin.data.ByteString;

/**
 * RestMessage is a message with MessageHeaders and a full entity.
 * RestMessage is immutable and can be shared safely by multiple threads.
 *
 * @see com.linkedin.r2.message.rest.RestRequest
 * @see com.linkedin.r2.message.rest.RestResponse
 *
 * @author Zhenkai Zhu
 */
public interface RestMessage extends MessageHeaders
{
  /**
   * Returns the whole entity for this message.
   * *
   * @return the entity for this message
   */
  ByteString getEntity();

  RestMessageBuilder<? extends RestMessageBuilder<?>> builder();
}
