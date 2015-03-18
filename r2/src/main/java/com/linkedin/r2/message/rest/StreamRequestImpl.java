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


import com.linkedin.data.ByteString;
import com.linkedin.r2.message.streaming.EntityStream;

import java.net.URI;
import java.util.List;
import java.util.Map;


/**
 * @author Chris Pettitt
 * @version $Revision$
 */
/* package private */ class StreamRequestImpl extends BaseRestMessage implements StreamRequest
{
  private final URI _uri;

  private final String _method;

  /* package private */ StreamRequestImpl(EntityStream stream, Map<String, String> headers, List<String> cookies, URI uri, String method)
  {
    super(stream, headers, cookies);

    assert uri != null;
    assert method != null;

    _uri = uri;
    _method = method;
  }

  public URI getURI()
  {
    return _uri;
  }

  @Override
  public StreamRequestBuilder requestBuilder()
  {
    return transformBuilder();
  }

  @Override
  public StreamRequestBuilder transformBuilder()
  {
    return new StreamRequestBuilder(this);
  }

  public String getMethod()
  {
    return _method;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o)
    {
      return true;
    }
    if (!(o instanceof StreamRequestImpl))
    {
      return false;
    }
    if (!super.equals(o))
    {
      return false;
    }

    StreamRequestImpl that = (StreamRequestImpl) o;
    return _method.equals(that._method) && _uri.equals(that._uri);
  }

  @Override
  public int hashCode()
  {
    int result = super.hashCode();
    result = 31 * result + _uri.hashCode();
    result = 31 * result + _method.hashCode();
    return result;
  }

  @Override
  public String toString()
  {
    StringBuilder builder = new StringBuilder();
    builder.append("StreamRequest[headers=")
        .append(getHeaders())
        .append("cookies=")
        .append(getCookies())
        .append(",uri=")
        .append(_uri)
        .append(",method=")
        .append(_method)
        .append("]");
    return builder.toString();
  }
}
