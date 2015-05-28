/*
   Copyright (c) 2015 LinkedIn Corp.

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

package com.linkedin.r2.filter.compression.streaming;

import com.linkedin.r2.filter.NextFilter;
import com.linkedin.r2.filter.message.rest.StreamFilter;
import com.linkedin.r2.message.RequestContext;
import com.linkedin.r2.message.rest.StreamRequest;
import com.linkedin.r2.message.rest.StreamResponse;
import com.linkedin.r2.message.rest.StreamResponseBuilder;
import com.linkedin.r2.message.streaming.EntityStream;
import com.linkedin.r2.message.streaming.EntityStreams;
import com.linkedin.r2.transport.http.common.HttpConstants;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Executor;


/**
 * @author Ang Xu
 */
public class ServerCompressionFilter implements StreamFilter
{
  private final Executor _executor;
  private final Set<EncodingType> _supportedEncoding;

  public ServerCompressionFilter(EncodingType[] supportedEncoding, Executor executor)
  {
    Set<EncodingType> supportedEncodingSet = new HashSet<EncodingType>(Arrays.asList(supportedEncoding));
    supportedEncodingSet.add(EncodingType.IDENTITY);
    supportedEncodingSet.add(EncodingType.ANY);
    _supportedEncoding = Collections.unmodifiableSet(supportedEncodingSet);
    _executor = executor;
  }

  @Override
  public void onRequest(StreamRequest req, RequestContext requestContext, Map<String, String> wireAttrs,
      NextFilter<StreamRequest, StreamResponse> nextFilter)
  {
    //Get client support for compression and flag compress if need be
    String responseCompression = req.getHeader(HttpConstants.ACCEPT_ENCODING);
    if (responseCompression != null)
    {
      requestContext.putLocalAttr(HttpConstants.ACCEPT_ENCODING, responseCompression);
    }

    nextFilter.onRequest(req, requestContext, wireAttrs);
  }

  @Override
  public void onResponse(StreamResponse res, RequestContext requestContext, Map<String, String> wireAttrs,
      NextFilter<StreamRequest, StreamResponse> nextFilter)
  {
    EncodingType selectedEncoding;
    String acceptEncoding = (String) requestContext.getLocalAttr(HttpConstants.ACCEPT_ENCODING);

    if (acceptEncoding == null)
    {
      // per RFC 2616, section 14.3, if no Accept-Encoding field is present, server SHOULD use
      // the "identity" content-coding if it's available. In our case, the "identity" content-encoding
      // should always be available.
      selectedEncoding = EncodingType.IDENTITY;
    }
    else
    {
      List<AcceptEncoding> parsedEncodings =
          AcceptEncoding.parseAcceptEncodingHeader(acceptEncoding, _supportedEncoding);
      selectedEncoding = AcceptEncoding.chooseBest(parsedEncodings);
    }

    //Check if there exists an acceptable encoding
    if (selectedEncoding != null)
    {
      StreamingCompressor compressor = selectedEncoding.getCompressor(_executor);
      if (compressor != null)
      {
        final EntityStream compressedStream = compressor.deflate(res.getEntityStream(), 0);
        final StreamResponseBuilder builder = res.builder();
        // make sure we don't include the original content-length header.
        if (builder.getHeader(HttpConstants.CONTENT_LENGTH) != null)
        {
          final Map<String, String> headers = removeContentLengthHeader(builder.getHeaders());
          builder.setHeaders(headers);
        }

        res = builder.addHeaderValue(HttpConstants.CONTENT_ENCODING, compressor.getContentEncodingName())
                     .build(compressedStream);
      }
    }
    else
    {
      //Not acceptable encoding status
      res = res.builder().setStatus(HttpConstants.NOT_ACCEPTABLE).build(EntityStreams.emptyStream());
    }
    nextFilter.onResponse(res, requestContext, wireAttrs);
  }

  @Override
  public void onError(Throwable ex, RequestContext requestContext, Map<String, String> wireAttrs,
      NextFilter<StreamRequest, StreamResponse> nextFilter)
  {
    nextFilter.onError(ex, requestContext, wireAttrs);
  }

  private Map<String, String> removeContentLengthHeader(Map<String, String> headers)
  {
    // case-insensitive map
    Map<String, String> newMap = new TreeMap<String, String>(String.CASE_INSENSITIVE_ORDER);
    newMap.putAll(headers);
    newMap.remove(HttpConstants.CONTENT_LENGTH);
    return newMap;
  }
}
