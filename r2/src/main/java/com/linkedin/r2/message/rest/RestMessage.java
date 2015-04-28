package com.linkedin.r2.message.rest;

import com.linkedin.data.ByteString;

/**
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
