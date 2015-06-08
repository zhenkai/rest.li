package com.linkedin.r2.message.rest;

import com.linkedin.r2.message.Message;

/**
 * @author Zhenkai Zhu
 */
public interface RestRequest extends Request, Message
{
  RestRequestBuilder builder();
}
