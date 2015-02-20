package com.linkedin.r2.message.rest;

/**
 * @author Zhenkai Zhu
 */
public interface RequestHeaders extends RestHeaders
{
  /**
   * Returns the REST method for this request.
   *
   * @return the REST method for this request
   * @see com.linkedin.r2.message.rest.RestMethod
   */
  String getMethod();
}
