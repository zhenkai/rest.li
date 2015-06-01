package com.linkedin.r2.message.rest;

/**
 * An object that contains details of a REST response.
 * RestResponse is a response with full entity.
 *
 * @author Chris Pettitt
 * @author Zhenkai Zhu
 */
public interface RestResponse extends Response, RestMessage
{
  RestResponseBuilder builder();
}
