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
import com.linkedin.r2.transport.common.StreamClient;
import com.linkedin.r2.transport.common.StreamResponseHandler;

/**
 * This is a slightly more complex Proxy that decodes the streamed request body before sending to downstream, and encodes
 * the streamed response body before sending back to upstream.
 *
 * Similar to the SimpleRelayProxy, back pressure is also achieved in the same fashion {@see SimpleRelayProxy}
 * @author Zhenkai Zhu
 */
public class CipherProxy implements StreamResponseHandler
{
  final private StreamClient _client;
  final private Cipher _cipher;

  public CipherProxy(StreamClient client, Cipher cipher)
  {
    _client = client;
    _cipher = cipher;
  }

  @Override
  public void handleStreamRequest(RestRequest request, RequestContext requestContext, final Callback<RestResponse> callback)
  {
    Processor requestProcessor = new RequestProcessor();

    // read encoded data from original request
    request.getEntityStream().setReader(requestProcessor);

    // write decoded data to new request
    EntityStream requestEntityStream = EntityStreams.newEntityStream(requestProcessor);
    RestRequestBuilder restRequestBuilder = request.builder();
    RestRequest decodedRequest = restRequestBuilder.build(requestEntityStream);

    _client.streamRestRequest(decodedRequest, requestContext, new Callback<RestResponse>()
    {
      @Override
      public void onError(Throwable e)
      {
        callback.onError(e);
      }

      @Override
      public void onSuccess(RestResponse result)
      {
        Processor responseProcessor = new ResponeProcessor();

        // read plain data from original response
        result.getEntityStream().setReader(responseProcessor);

        // write encoded data to new response
        EntityStream responseEntityStream = EntityStreams.newEntityStream(responseProcessor);
        RestResponseBuilder restResponseBuilder = result.builder();
        RestResponse encodedResponse = restResponseBuilder.build(responseEntityStream);
        callback.onSuccess(encodedResponse);
      }
    });
  }

  private class RequestProcessor extends Processor
  {
    @Override
    protected ByteString process(ByteString input)
    {
      return _cipher.decode(input);
    }
  }

  private class ResponeProcessor extends Processor
  {
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
    private WriteHandle _writeHandle;
    private ReadHandle _readHandle;

    // ===== Reader part =====
    @Override
    public void onInit(ReadHandle readHandle)
    {
      _readHandle = readHandle;
    }

    @Override
    public void onReadPossible(ByteString data)
    {
      // assume processedData.length() <= data.length()
      ByteString processedData = process(data);
      // we can safely write to E2 as the data is supplied to us because we have received permission from
      // Reader of E2, and requested more from Writer of E1
      _writeHandle.write(processedData);
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
    public void onInit(WriteHandle writeHandle)
    {
      _writeHandle = writeHandle;
    }

    @Override
    public void onWritePossible(int byteNum)
    {
      // Reader of E2 says we can write more, so let's request more from Writer of E1
      _readHandle.read(byteNum);
    }

    protected abstract ByteString process(ByteString input);
  }

  public interface Cipher
  {
    ByteString decode(ByteString encrypted);
    ByteString encode(ByteString plain);
  }
}
