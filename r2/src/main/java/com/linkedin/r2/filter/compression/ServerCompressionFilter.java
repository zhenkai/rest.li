/*
   Copyright (c) 2013 LinkedIn Corp.

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

package com.linkedin.r2.filter.compression;

import com.linkedin.common.callback.Callback;
import com.linkedin.r2.filter.compression.streaming.CompositeWriter;
import com.linkedin.r2.filter.compression.streaming.PartialReader;
import com.linkedin.r2.filter.compression.streaming.StreamingCompressor;
import com.linkedin.r2.filter.message.rest.StreamFilter;
import com.linkedin.r2.message.rest.StreamException;
import com.linkedin.r2.message.rest.StreamRequest;
import com.linkedin.r2.message.rest.StreamResponse;
import com.linkedin.r2.message.rest.StreamResponseBuilder;
import com.linkedin.r2.message.streaming.EntityStream;
import com.linkedin.r2.message.streaming.EntityStreams;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import java.util.TreeMap;
import java.util.concurrent.Executor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.r2.filter.NextFilter;
import com.linkedin.r2.message.RequestContext;
import com.linkedin.r2.transport.http.common.HttpConstants;

/**
 *
 * Filter class for server to negotiate acceptable compression formats from clients
 * and compresses the response with the relevant headers accordingly.
 *
 * @author erli
 * @author Ang Xu
 */
public class ServerCompressionFilter implements StreamFilter
{
  private static final Logger LOG = LoggerFactory.getLogger(ServerCompressionFilter.class);

  private final Set<EncodingType> _supportedEncoding;
  private final Executor _executor;
  private final int _compressThreshold;


  /** Takes a comma delimited string containing standard
   * HTTP encoding headers and instantiates server compression
   * support for the said encoding types.
   * @param acceptedFilters
   */
  public ServerCompressionFilter(String acceptedFilters, Executor executor)
  {
    this(acceptedFilters, executor, 0);
  }

  public ServerCompressionFilter(String acceptedFilters, Executor executor, int compressThreshold)
  {
    this(AcceptEncoding.parseAcceptEncoding(acceptedFilters), executor, compressThreshold);
  }

  /** Instantiates a compression filter
   * that supports the compression methods in the given set in argument.
   * @param supportedEncoding
   */
  public ServerCompressionFilter(EncodingType[] supportedEncoding, Executor executor, int compressThreshold)
  {
    _supportedEncoding = new HashSet<EncodingType>(Arrays.asList(supportedEncoding));
    _supportedEncoding.add(EncodingType.IDENTITY);
    _supportedEncoding.add(EncodingType.ANY);
    _executor = executor;
    _compressThreshold = compressThreshold;
  }

  /**
   * Handles compression tasks for incoming requests
   */
  @Override
  public void onRequest(StreamRequest req, RequestContext requestContext, Map<String, String> wireAttrs,
      NextFilter<StreamRequest, StreamResponse> nextFilter)
  {
    //Check if the request is compressed, if so, decompress
    String requestContentEncoding = req.getHeader(HttpConstants.CONTENT_ENCODING);
    if (requestContentEncoding != null)
    {
      //This must be a specific compression type other than *
      EncodingType encoding = EncodingType.get(requestContentEncoding.trim().toLowerCase());
      if (encoding == null || encoding == EncodingType.ANY)
      {
        Exception ex = new IllegalArgumentException("Unsupported Content-encoding type: " + requestContentEncoding);
        LOG.error(ex.getMessage(), ex.getCause());
        StreamResponse streamResponse =
            new StreamResponseBuilder().setStatus(HttpConstants.UNSUPPORTED_MEDIA_TYPE).build(EntityStreams.emptyStream());
        nextFilter.onError(new StreamException(streamResponse, ex), requestContext, wireAttrs);
        return;
      }
      //Process the correct content-encoding types only
      StreamingCompressor compressor = encoding.getCompressor(_executor);
      EntityStream uncompressedStream = compressor.inflate(req.getEntityStream());
      req = req.builder().build(uncompressedStream);
    }

    //Get client support for compression and flag compress if need be
    String responseCompression = req.getHeader(HttpConstants.ACCEPT_ENCODING);
    if (responseCompression == null)
    {
      // per RFC 2616, section 14.3, if no Accept-Encoding field is present in a request,
      // server SHOULD use "identity" content-encoding if it is available.
      responseCompression = EncodingType.IDENTITY.getHttpName();
    }
    requestContext.putLocalAttr(HttpConstants.ACCEPT_ENCODING, responseCompression);
    nextFilter.onRequest(req, requestContext, wireAttrs);
  }

  /**
   * Optionally compresses outgoing response
   * */
  @Override
  public void onResponse(final StreamResponse res, final RequestContext requestContext, final Map<String, String> wireAttrs,
      final NextFilter<StreamRequest, StreamResponse> nextFilter)
  {
    StreamResponse response = res;
    try
    {
      String responseCompression = (String) requestContext.getLocalAttr(HttpConstants.ACCEPT_ENCODING);
      if (responseCompression == null)
      {
        throw new CompressionException(CompressionConstants.UNKNOWN_ENCODING);
      }

      List<AcceptEncoding> parsedEncodings = AcceptEncoding.parseAcceptEncodingHeader(responseCompression, _supportedEncoding);
      EncodingType selectedEncoding = AcceptEncoding.chooseBest(parsedEncodings);

      //Check if there exists an acceptable encoding
      if (selectedEncoding == null)
      {
        //Not acceptable encoding status
        response = new StreamResponseBuilder().setStatus(HttpConstants.NOT_ACCEPTABLE).build(EntityStreams.emptyStream());
      }
      else if (selectedEncoding != EncodingType.IDENTITY)
      {
        final StreamingCompressor compressor = selectedEncoding.getCompressor(_executor);
        PartialReader reader = new PartialReader(_compressThreshold, new Callback<EntityStream[]>()
        {
          @Override
          public void onError(Throwable ex)
          {
            nextFilter.onError(ex, requestContext, wireAttrs);
          }

          @Override
          public void onSuccess(EntityStream[] results)
          {
            if (results.length == 1) // entity stream is less than threshold
            {
              StreamResponse response = res.builder().build(results[0]);
              nextFilter.onResponse(response, requestContext, wireAttrs);
            }
            else
            {
              EntityStream compressedStream = compressor.deflate(EntityStreams.newEntityStream(new CompositeWriter(results)));
              StreamResponseBuilder builder = res.builder();
              // remove original content-length header if presents.
              if (builder.getHeader(HttpConstants.CONTENT_LENGTH) != null)
              {
                Map<String, String> headers = stripHeaders(builder.getHeaders(), HttpConstants.CONTENT_LENGTH);
                builder.setHeaders(headers);
              }
              StreamResponse response = builder.addHeaderValue(HttpConstants.CONTENT_ENCODING, compressor.getContentEncodingName())
                  .build(compressedStream);
              nextFilter.onResponse(response, requestContext, wireAttrs);
            }
          }
        });
        res.getEntityStream().setReader(reader);
        return;
      }
    }
    catch (CompressionException e)
    {
      LOG.error(e.getMessage(), e.getCause());
    }

    nextFilter.onResponse(response, requestContext, wireAttrs);
  }


  @Override
  public void onError(Throwable ex, RequestContext requestContext, Map<String, String> wireAttrs,
      NextFilter<StreamRequest, StreamResponse> nextFilter)
  {
    nextFilter.onError(ex, requestContext, wireAttrs);
  }

  private Map<String, String> stripHeaders(Map<String, String> headerMap, String...headers)
  {
    Map<String, String> newMap = new TreeMap<String, String>(String.CASE_INSENSITIVE_ORDER);
    newMap.putAll(headerMap);
    for (String header : headers)
    {
      newMap.remove(header);
    }
    return newMap;
  }
}