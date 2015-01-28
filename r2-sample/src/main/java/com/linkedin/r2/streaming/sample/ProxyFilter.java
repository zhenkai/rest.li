package com.linkedin.r2.streaming.sample;

import com.linkedin.data.ByteString;
import com.linkedin.r2.filter.NextFilter;
import com.linkedin.r2.filter.message.rest.RestResponseFilter;
import com.linkedin.r2.message.RequestContext;
import com.linkedin.r2.message.rest.RestRequest;
import com.linkedin.r2.message.rest.RestResponse;
import com.linkedin.r2.message.streaming.EntityStream;
import com.linkedin.r2.message.streaming.Observer;
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
public class ProxyFilter implements RestResponseFilter
{
  private static final Logger _log = LoggerFactory.getLogger(ProxyFilter.class);

  @Override
  public void onRestResponse(RestResponse res,
                             RequestContext requestContext,
                             Map<String, String> wireAttrs,
                             NextFilter<RestRequest, RestResponse> nextFilter)
  {
    EntityStream entityStream = res.getEntityStream();
    entityStream.addObserver(new Observer()
    {
      private long startTime;
      private long bytesNum = 0;

      @Override
      public void onReadPossible(ByteString data)
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
  public void onRestError(Throwable ex,
                          RequestContext requestContext,
                          Map<String, String> wireAttrs,
                          NextFilter<RestRequest, RestResponse> nextFilter)
  {
    _log.error("Encountered failure before anything has been streamed", ex);
  }
}
