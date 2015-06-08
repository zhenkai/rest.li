/*
   Copyright (c) 2012 LinkedIn Corp.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

/* $Id$ */
package com.linkedin.r2.message.rest;

import com.linkedin.r2.transport.http.common.HttpConstants;

/**
 * @author Chris Pettitt
 * @version $Revision$
 */
/* package private */ abstract class BaseResponseBuilder<B extends BaseResponseBuilder<B>>
        extends BaseRestMessageBuilder<B>
        implements RestMessageBuilder<B>
{
  private int _status = RestStatus.OK;

  /**
   * Constructs a new builder with no initial values.
   */
  public BaseResponseBuilder() {}

  /**
   * Copies the values from the supplied response. Changes to this builder will not be reflected
   * in the original message.
   *
   * @param response the response to copy
   */
  public BaseResponseBuilder(Response response)
  {
    super(response);
    setStatus(response.getStatus());
  }

  /**
   * Sets the status for this response.
   *
   * @param status the status code to set
   * @return this builder
   * @see com.linkedin.r2.message.rest.RestStatus
   */
  public B setStatus(int status)
  {
    _status = status;
    return thisBuilder();
  }

  /**
   * Returns the status for this response.
   *
   * @return the status for this response
   * @see com.linkedin.r2.message.rest.RestStatus
   */
  public int getStatus()
  {
    return _status;
  }


  @Override
  protected void validateCookieHeader(String name)
  {
    if (name.equalsIgnoreCase(HttpConstants.RESPONSE_COOKIE_HEADER_NAME))
    {
      String message = String.format(
          "Header %s are not allowed to be added as a response header.",
          HttpConstants.RESPONSE_COOKIE_HEADER_NAME);
      throw new IllegalArgumentException(message);
    }
  }
}
