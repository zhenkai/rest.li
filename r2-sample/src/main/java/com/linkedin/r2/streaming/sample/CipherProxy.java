package com.linkedin.r2.streaming.sample;

import com.linkedin.common.callback.Callback;
import com.linkedin.data.ByteString;
import com.linkedin.r2.message.RequestContext;
import com.linkedin.r2.message.rest.RestRequest;
import com.linkedin.r2.message.rest.RestRequestBuilder;
import com.linkedin.r2.message.rest.RestResponse;
import com.linkedin.r2.message.rest.RestResponseBuilder;
import com.linkedin.r2.message.streaming.EntityStream;
import com.linkedin.r2.message.streaming.EntityStreams;
import com.linkedin.r2.message.streaming.ReadHandle;
import com.linkedin.r2.message.streaming.Reader;
import com.linkedin.r2.message.streaming.WriteHandle;
import com.linkedin.r2.message.streaming.Writer;
import com.linkedin.r2.transport.common.Client;
import com.linkedin.r2.transport.common.RestRequestHandler;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * This is a slightly more complex Proxy that decodes the streamed request body before sending to downstream, and encodes
 * the streamed response body before sending back to upstream.
 *
 * Similar to the SimpleRelayProxy, back pressure is also achieved in the same fashion {@see SimpleRelayProxy}
 * @author Zhenkai Zhu
 */
public class CipherProxy implements RestRequestHandler
{
  final private Client _client;
  final private Cipher _cipher;

  public CipherProxy(Client client, Cipher cipher)
  {
    _client = client;
    _cipher = cipher;
  }

  @Override
  public void handleRequest(RestRequest request, RequestContext requestContext, final Callback<RestResponse> callback)
  {
    // processor will read encoded data from original request entity stream
    Processor requestProcessor = new RequestProcessor(request.getEntityStream());

    // processor will write decoded data to new request entity stream
    EntityStream requestEntityStream = EntityStreams.newEntityStream(requestProcessor);
    RestRequestBuilder restRequestBuilder = request.builder();
    RestRequest decodedRequest = restRequestBuilder.build(requestEntityStream);

    _client.restRequest(decodedRequest, requestContext, new Callback<RestResponse>()
    {
      @Override
      public void onError(Throwable e)
      {
        callback.onError(e);
      }

      @Override
      public void onSuccess(RestResponse result)
      {
        // processor will read plain data from original response entity stream
        Processor responseProcessor = new ResponseProcessor(result.getEntityStream());

        // processor will write encoded data to new response entity stream
        EntityStream responseEntityStream = EntityStreams.newEntityStream(responseProcessor);
        RestResponseBuilder restResponseBuilder = result.builder();
        RestResponse encodedResponse = restResponseBuilder.build(responseEntityStream);
        callback.onSuccess(encodedResponse);
      }
    });
  }

  private class RequestProcessor extends Processor
  {
    RequestProcessor(EntityStream originalStream)
    {
      super(originalStream);
    }

    @Override
    protected ByteString process(ByteString input)
    {
      return _cipher.decode(input);
    }
  }

  private class ResponseProcessor extends Processor
  {
    ResponseProcessor(EntityStream originalStream)
    {
      super(originalStream);
    }

    @Override
    protected ByteString process(ByteString input)
    {
      return _cipher.encode(input);
    }
  }

  /**
   * A Processor reads from EntityStream E1, process the data, and writes to EntityStream E2. Conceptually, it links
   * two EntityStreams into a single EntityStream with additional processing of data.
   *
   * The data flow of E1->Processor->E2 is driven by the Reader of E2, which is the ultimate data reader.
   *
   * This could be pull up to be a public convenient class.
   */
  private static abstract class Processor implements Reader, Writer
  {
    final private EntityStream _originalStream;
    private WriteHandle _writeHandle;
    private ReadHandle _readHandle;
    final private Queue<ByteString> _queue;

    Processor(EntityStream originalStream)
    {
      _originalStream = originalStream;
      _queue = new ConcurrentLinkedQueue<ByteString>();
    }

    // ===== Reader part =====
    @Override
    public void onInit(ReadHandle readHandle)
    {
      _readHandle = readHandle;
      _readHandle.read(100);
    }

    @Override
    public void onDataAvailable(ByteString data)
    {
      ByteString processedData = process(data);
      _queue.offer(processedData);
      doWrite();
    }

    @Override
    public void onDone()
    {
      _writeHandle.done();
    }

    @Override
    public void onError(Throwable e)
    {
      _writeHandle.error(e);
    }

    // ===== Writer part =====
    @Override
    public void onInit(WriteHandle writeHandle, int chunkSize)
    {
      _writeHandle = writeHandle;
      // this means the reader of the new stream is ready, and told us the desired chunkSize,
      // so now we can hook up with the original stream with the same chunkSize
      _originalStream.setReader(this, chunkSize);
    }

    @Override
    public void onWritePossible()
    {
      doWrite();
    }

    private void doWrite()
    {
      while(_writeHandle.isWritable() && _queue.peek() != null)
      {
        _writeHandle.write(_queue.poll());
        _readHandle.read(1);
      }
    }

    protected abstract ByteString process(ByteString input);
  }

  public interface Cipher
  {
    ByteString decode(ByteString encrypted);
    ByteString encode(ByteString plain);
  }
}
