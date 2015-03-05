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


import com.linkedin.r2.message.Message;

import java.util.List;
import java.util.Map;


/**
 * An object that represents a REST message, either a request or a response.<p/>
 *
 * Instances of RestMessage are immutable and thread-safe. It is possible to clone an existing
 * RestMessage, modify details in the copy, and create a new RestMessage instance that has the
 * concrete type of the original message (request or response) using the {@link #restBuilder()}
 * method.
 *
 * @see RestRequest
 * @see RestResponse
 * @author Chris Pettitt
 * @version $Revision$
 */
public interface RestMessage extends RestHeaders, Message
{
  /**
   * Returns a {@link RestMessageBuilder}, which provides a means of constructing a new message using
   * this message as a starting point. Changes made with the builder are not reflected by this
   * message instance.
   *
   * @return a builder for this message
   */
  RestMessageBuilder<? extends RestMessageBuilder<?>> restBuilder();
}
