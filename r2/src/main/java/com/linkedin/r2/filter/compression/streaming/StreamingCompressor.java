package com.linkedin.r2.filter.compression.streaming;

import com.linkedin.r2.filter.compression.CompressionException;
import com.linkedin.r2.message.streaming.EntityStream;
import java.io.InputStream;
import java.util.zip.DataFormatException;


/**
 * @author Ang Xu
 */
public interface StreamingCompressor
{
  /**
   * @return Corresponding value for the content-encoding for the implemented
   * compression method.
   * */
  String getContentEncodingName();

  /** Decompression function.
   * @param input EntityStream to be decompressed
   * @return Newly created EntityStream of decompressed of data, or null if error
   * */
  EntityStream inflate(EntityStream input);

  /** Compress function.
   * @param input EntityStream to be compressed
   * @return Newly created EntityStream of compressed data, or null if error
   * */
  EntityStream deflate(EntityStream input);
}
