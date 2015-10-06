package com.linkedin.r2.streaming.sample;

import com.linkedin.data.ByteString;
import com.linkedin.r2.filter.NextFilter;
import com.linkedin.r2.filter.message.rest.StreamResponseFilter;
import com.linkedin.r2.message.RequestContext;
import com.linkedin.r2.message.stream.StreamRequest;
import com.linkedin.r2.message.stream.StreamResponse;
import com.linkedin.r2.message.entitystream.EntityStream;
import com.linkedin.r2.message.entitystream.Observer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * This example shows how the Observer can be used to implement Filters.
 *
 * This ProxyFilter simply records the status, total streamed bytes, and total time for a response.
 *
 * @author Zhenkai Zhu
 */
public class LoggingFilter implements StreamResponseFilter
{
  private static final Logger _log = LoggerFactory.getLogger(LoggingFilter.class);

  @Override
  public void onResponse(StreamResponse res,
                             RequestContext requestContext,
                             Map<String, String> wireAttrs,
                             NextFilter<StreamRequest, StreamResponse> nextFilter)
  {
    EntityStream entityStream = res.getEntityStream();
    entityStream.addObserver(new Observer()
    {
      private long startTime;
      private long bytesNum = 0;

      @Override
      public void onDataAvailable(ByteString data)
      {
        if (bytesNum == 0)
        {
          startTime = System.nanoTime();
        }

        bytesNum += data.length();
      }

      @Override
      public void onDone()
      {
        long stopTime = System.nanoTime();
        _log.info("Status: success. Total bytes streamed: " + bytesNum +
            ". Total stream time: " + (stopTime - startTime) + " nano seconds.");
      }

      @Override
      public void onError(Throwable e)
      {
        long stopTime = System.nanoTime();
        _log.error("Status: failed. Total bytes streamed: " + bytesNum +
            ". Total stream time before failure: " + (stopTime - startTime) + " nano seconds.");
      }
    });
  }

  @Override
  public void onError(Throwable ex,
                          RequestContext requestContext,
                          Map<String, String> wireAttrs,
                          NextFilter<StreamRequest, StreamResponse> nextFilter)
  {
    _log.error("Encountered failure before anything has been streamed", ex);
  }
}
