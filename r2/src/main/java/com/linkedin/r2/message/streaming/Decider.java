package com.linkedin.r2.message.streaming;

import com.linkedin.r2.message.rest.RestHeaders;

/**
 * @author Zhenkai Zhu
 */
public interface Decider<T extends RestHeaders>
{
  boolean shouldStream(T headers);
}
