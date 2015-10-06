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
package com.linkedin.r2.filter.message.rest;

import com.linkedin.r2.filter.Filter;
import com.linkedin.r2.filter.NextFilter;
import com.linkedin.r2.message.RequestContext;
import com.linkedin.r2.message.stream.StreamRequest;
import com.linkedin.r2.message.stream.StreamResponse;

import java.util.Map;

/**
 * A filter that processes {@link StreamResponse}s.
 *
 * @author Chris Pettitt
 * @version $Revision$
 */
public interface StreamResponseFilter extends Filter
{
  /**
   * Method to be invoked for each {@link StreamResponse} message.
   *
   * @param res the {@link com.linkedin.r2.message.stream.StreamResponse} message.
   * @param requestContext the {@link RequestContext} of the request.
   * @param wireAttrs the wire attributes of the response.
   * @param nextFilter the next filter in the chain.  Concrete implementations should invoke
   *                   {@link NextFilter#onResponse} to continue the filter chain.
   */
  void onResponse(StreamResponse res,
                      RequestContext requestContext,
                      Map<String, String> wireAttrs,
                      NextFilter<StreamRequest, StreamResponse> nextFilter);

  /**
   * Method to be invoked when an error is encountered.
   *
   * @param ex the {@link Throwable} representation of the error.
   * @param requestContext the {@link RequestContext} of the request.
   * @param wireAttrs the wire attributes of the response (if any).
   * @param nextFilter the next filter in the chain.  Concrete implementations should invoke
   *                   {@link NextFilter#onError} to continue the filter chain.
   */
  void onError(Throwable ex,
                   RequestContext requestContext,
                   Map<String, String> wireAttrs,
                   NextFilter<StreamRequest, StreamResponse> nextFilter);
}
