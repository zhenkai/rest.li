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

import java.util.LinkedList;
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
    final BufferedProcessor requestProcessor = new RequestProcessor(request.getEntityStream(), 100);

    // processor will write decoded data to new request entity stream
    // pass in the original entity stream so the events for the two streams are processed by the same thread
    EntityStream decodedStream = EntityStreams.newEntityStream(requestProcessor, request.getEntityStream());
    RestRequestBuilder restRequestBuilder = request.builder();
    RestRequest decodedRequest = restRequestBuilder.build(decodedStream);

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
        BufferedProcessor responseProcessor = new ResponseProcessor(result.getEntityStream(), 100);

        // processor will write encoded data to new response entity stream
        // pass in the original entity stream so the events for the two streams are processed by the same thread
        EntityStream encodedStream = EntityStreams.newEntityStream(responseProcessor, result.getEntityStream());
        RestResponseBuilder restResponseBuilder = result.builder();
        RestResponse encodedResponse = restResponseBuilder.build(encodedStream);
        callback.onSuccess(encodedResponse);
      }
    });
  }

  private class RequestProcessor extends BufferedProcessor
  {
    RequestProcessor(EntityStream originalStream, int bufferCapacity)
    {
      super(originalStream, bufferCapacity);
    }

    @Override
    protected ByteString process(ByteString input)
    {
      return _cipher.decode(input);
    }
  }

  private class ResponseProcessor extends BufferedProcessor
  {
    ResponseProcessor(EntityStream originalStream, int bufferCapacity)
    {
      super(originalStream, bufferCapacity);
    }

    @Override
    protected ByteString process(ByteString input)
    {
      return _cipher.encode(input);
    }
  }

  /**
   * This could be pull up to be a public convenient class.
   */
  private static abstract class BufferedProcessor implements Reader, Writer
  {
    final private EntityStream _originalStream;
    private WriteHandle _writeHandle;
    private ReadHandle _readHandle;
    final private Queue<ByteString> _queue;
    final private int _bufferCapacity;

    BufferedProcessor(EntityStream originalStream, int bufferCapacity)
    {
      _originalStream = originalStream;
      _queue = new LinkedList<ByteString>();
      _bufferCapacity = bufferCapacity;
    }

    // ===== Reader part =====
    @Override
    public void onInit(ReadHandle readHandle)
    {
      _readHandle = readHandle;
      // we are willing to buffer up to _bufferCapacity chunks
      _readHandle.read(_bufferCapacity);
    }

    @Override
    public void onDataAvailable(ByteString data)
    {
      ByteString processedData = process(data);

      // no need to synchronize because all events all handled by the same thread
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
      // so now we can hook up with the original stream with the same chunkSize (to avoid extra copy
      // as a result of different size of ByteString)
      _originalStream.setReader(this, chunkSize);
    }

    @Override
    public void onWritePossible()
    {
      // no need to synchronize because all events are handled by the same thread
      doWrite();
    }

    // no need to synchronize because all events are handled in the same thread and this is always called in
    // an event
    private void doWrite()
    {
      while(_writeHandle.isWritable() && _queue.peek() != null)
      {
        _writeHandle.write(_queue.poll());
        _readHandle.read(1);
      }
    }

    /**
     * Process input and returns an output with equal or smaller length
     * @param input the input ByteString
     * @return the output ByteString with equal or smaller length
     */
    protected abstract ByteString process(ByteString input);
  }

  public interface Cipher
  {
    ByteString decode(ByteString encrypted);
    ByteString encode(ByteString plain);
  }
}
