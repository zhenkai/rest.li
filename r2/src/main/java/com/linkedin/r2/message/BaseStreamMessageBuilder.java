package com.linkedin.r2.message;


/**
 * @author Zhenkai Zhu
 */
public abstract class BaseStreamMessageBuilder<B extends BaseStreamMessageBuilder<B>> implements StreamMessageBuilder<B>
{
  @SuppressWarnings("unchecked")
  protected B thisBuilder()
  {
    return (B)this;
  }
}
