package test.r2.perf.client;

import com.linkedin.data.ByteString;
import com.linkedin.r2.filter.R2Constants;
import com.linkedin.r2.message.rest.StreamRequest;
import com.linkedin.r2.message.rest.StreamRequestBuilder;
import com.linkedin.r2.message.streaming.EntityStreams;
import com.linkedin.r2.message.streaming.WriteHandle;
import com.linkedin.r2.message.streaming.Writer;
import test.r2.perf.Generator;
import test.r2.perf.PerfStreamWriter;
import test.r2.perf.StringGenerator;

import java.net.URI;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @auther Zhenkai Zhu
 */

public class StreamRequestGenerator implements Generator<StreamRequest>
{
  private final URI _uri;
  private final int _msgSize;
  private final AtomicInteger _msgCounter;


  public StreamRequestGenerator(URI uri, int numMsgs, int msgSize)
  {
    _uri = uri;
    _msgCounter = new AtomicInteger(numMsgs);
    _msgSize = msgSize;
  }

  @Override
  public StreamRequest nextMessage()
  {
    if (_msgCounter.getAndDecrement() > 0)
    {
      return new StreamRequestBuilder(_uri)
          .setMethod("POST")
          .build(EntityStreams.newEntityStream(new PerfStreamWriter(_msgSize)));
    }
    else
    {
      return null;
    }
  }
}
