package com.linkedin.r2.message.rest;

/**
 * An object that contains details of a REST request.
 * RestRequest is a request with full entity.
 *
 * @author Chris Pettitt
 * @author Zhenkai Zhu
 */
public interface RestRequest extends Request, RestMessage
{
  RestRequestBuilder builder();
}
