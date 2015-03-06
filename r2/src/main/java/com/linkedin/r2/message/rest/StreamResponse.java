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


import java.util.Collections;

import com.linkedin.r2.message.Response;
import com.linkedin.r2.message.streaming.EntityStreams;


/**
 * An object that contains details of a REST response.<p/>
 *
 * New instances can be created using the
 * {@link StreamResponseBuilder}. An existing RestResponse can be used as a prototype for
 * building a new StreamResponse using the {@link #builder()} method.
 *
 * @author Chris Pettitt
 * @version $Revision$
 */
public interface StreamResponse extends RestMessage, Response, ResponseHeaders
{
  StreamResponse NO_RESPONSE = new StreamResponseImpl(
      EntityStreams.emptyStream(), Collections.<String, String>emptyMap(), Collections.<String>emptyList(), 0);

  /**
   * Returns a {@link StreamResponseBuilder}, which provides a means of constructing a new
   * response using this response as a starting point. Changes made with the builder are
   * not reflected by this response instance.
   *
   * @return a builder for this response
   */
  @Override
  StreamResponseBuilder builder();
}
