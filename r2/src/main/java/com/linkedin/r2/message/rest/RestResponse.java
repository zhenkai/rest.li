package com.linkedin.r2.message.rest;

/**
 * @author Zhenkai Zhu
 */
public interface RestResponse extends Response, RestMessage
{
  RestResponseBuilder builder();
}
