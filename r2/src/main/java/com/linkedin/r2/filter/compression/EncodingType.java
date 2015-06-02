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

import com.linkedin.data.ByteString;
import com.linkedin.r2.message.streaming.WriteHandle;
import com.linkedin.r2.message.streaming.Writer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
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
      default:
        return null;
    }
  }

  public static EncodingType get(String httpName)
  {
    EncodingType result = REVERSE_MAP.get(httpName);
    if (result == null)
    {
      throw new IllegalArgumentException(CompressionConstants.UNSUPPORTED_ENCODING + httpName);
    }
    return result;
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

//public enum EncodingType
//{
//  //NOTE: declaration order implicitly defines preference order
//  GZIP(new GzipCompressor()),
//  DEFLATE(new DeflateCompressor()),
//  BZIP2(new Bzip2Compressor()),
//  SNAPPY(new SnappyCompressor()),
//  IDENTITY("identity"),
//  ANY("*");
//
//  private final String httpName;
//  private final Compressor compressor;
//  private static final Map<String,EncodingType> _reverseMap;
//
//  //Initialize the reverse map for lookups
//  static
//  {
//    Map<String, EncodingType> reverseMap = new HashMap<String, EncodingType>();
//    for(EncodingType t : EncodingType.values())
//    {
//      reverseMap.put(t.getHttpName(), t);
//    }
//    _reverseMap = Collections.unmodifiableMap(reverseMap);
//  }
//
//  /**
//   * @param httpName Http value for this encoding
//   */
//  EncodingType(String httpName)
//  {
//    this.httpName = httpName;
//    compressor = null;
//  }
//
//  /**
//   * @param compressor Compressor associated with this encoding enum
//   */
//  EncodingType(Compressor compressor)
//  {
//    this.compressor = compressor;
//    httpName = compressor.getContentEncodingName();
//  }
//
//  /**
//   * @return Http value for this enum
//   */
//  public String getHttpName()
//  {
//    return httpName;
//  }
//
//  /**
//   * Returns the compressor of this compression method
//   */
//  public Compressor getCompressor()
//  {
//    return compressor;
//  }
//
//  /**
//   * Returns the encoding type corresponding to the encoding name.
//   * Throws {@link IllegalArgumentException} if there is no corresponding enum.
//   *
//   * @param compressionHeader Http encoding type as string.
//   * @return associated enum value.
//   */
//  public static EncodingType get(String compressionHeader)
//  {
//    EncodingType result = _reverseMap.get(compressionHeader);
//    if (result == null)
//    {
//      throw new IllegalArgumentException(CompressionConstants.UNSUPPORTED_ENCODING + compressionHeader);
//    }
//    return result;
//  }
//
//  /**
//   * Checks whether the encoding is supported.
//   *
//   * @param encodingName Http encoding name.
//   * @return true if the encoding is supported
//   */
//  public static boolean isSupported(String encodingName)
//  {
//    return _reverseMap.containsKey(encodingName);
//  }
//
//  /**
//   * @return if this encoding has a compressor. Generally, speaking, this is false
//   * for ANY (*).
//   */
//  public boolean hasCompressor()
//  {
//    return getCompressor() != null;
//  }
//}
