package com.linkedin.r2.message.rest;

/**
 * @author Zhenkai Zhu
 */
public interface RestRequest extends Request, RestMessage
{
  RestRequestBuilder builder();
}
