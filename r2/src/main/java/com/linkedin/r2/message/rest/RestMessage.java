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
package com.linkedin.r2.message.rest;

import com.linkedin.data.ByteString;

/**
 * RestMessage is a message with MessageHeaders and a full entity.
 * RestMessage is immutable and can be shared safely by multiple threads.
 *
 * @see com.linkedin.r2.message.rest.RestRequest
 * @see com.linkedin.r2.message.rest.RestResponse
 *
 * @author Zhenkai Zhu
 */
public interface RestMessage extends MessageHeaders
{
  /**
   * Returns the whole entity for this message.
   * *
   * @return the entity for this message
   */
  ByteString getEntity();

  RestMessageBuilder<? extends RestMessageBuilder<?>> builder();
}
