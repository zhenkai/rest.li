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

    // read from encoded request
    request.getEntityStream().setReader(requestProcessor);

    // write to decoded request
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

        // read from plain response
        result.getEntityStream().setReader(responseProcessor);

        // write to encoded response
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

  private abstract static class Processor implements Reader, Writer
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
      // we can write as this data is provided to us
      // because we signaled via _readHandle that we want more
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
      // we can write more, so let's request to read more
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
