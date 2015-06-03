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

import com.linkedin.r2.filter.compression.streaming.NoopCompressor;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;

import com.linkedin.r2.filter.compression.streaming.StreamingCompressor;
import com.linkedin.r2.filter.compression.streaming.GzipCompressor;
import com.linkedin.r2.filter.compression.streaming.DeflateCompressor;
import com.linkedin.r2.filter.compression.streaming.SnappyCompressor;
import com.linkedin.r2.filter.compression.streaming.Bzip2Compressor;

/**
 * @author Ang Xu
 */
public enum EncodingType
{
  GZIP("gzip"),
  DEFLATE("deflate"),
  SNAPPY_FRAMED("x-snappy-framed"),
  BZIP2("bzip2"),
  IDENTITY("identity"),
  ANY("*");

  private static final Map<String,EncodingType> REVERSE_MAP;

  static
  {
    Map<String, EncodingType> reverseMap = new HashMap<String, EncodingType>();
    for(EncodingType t : EncodingType.values())
    {
      reverseMap.put(t.getHttpName(), t);
    }
    REVERSE_MAP = Collections.unmodifiableMap(reverseMap);
  }

  private final String _httpName;

  EncodingType(String httpName)
  {
    _httpName = httpName;
  }

  public String getHttpName()
  {
    return _httpName;
  }

  public StreamingCompressor getCompressor(Executor executor)
  {
    switch (this)
    {
      case GZIP:
        return new GzipCompressor(executor);
      case DEFLATE:
        return new DeflateCompressor(executor);
      case BZIP2:
        return new Bzip2Compressor(executor);
      case SNAPPY_FRAMED:
        return new SnappyCompressor(executor);
      case IDENTITY:
        return new NoopCompressor();
      default:
        return null;
    }
  }

  public static EncodingType get(String httpName)
  {
    return REVERSE_MAP.get(httpName);
  }

  /**
   * Checks whether the encoding is supported.
   *
   * @param encodingName Http encoding name.
   * @return true if the encoding is supported
   */
  public static boolean isSupported(String encodingName)
  {
    return REVERSE_MAP.containsKey(encodingName);
  }

}
