package com.linkedin.r2.transport.common.bridge.server;

import com.linkedin.common.callback.Callback;
import com.linkedin.data.ByteString;
import com.linkedin.r2.message.RequestContext;
import com.linkedin.r2.message.rest.RestRequestBuilder;
import com.linkedin.r2.message.rest.RestResponse;
import com.linkedin.r2.message.rest.StreamRequest;
import com.linkedin.r2.message.rest.StreamResponse;
import com.linkedin.r2.message.streaming.ByteStringWriter;
import com.linkedin.r2.message.streaming.EntityStreams;
import com.linkedin.r2.message.streaming.FullEntityReader;
import com.linkedin.r2.message.streaming.Reader;
import com.linkedin.r2.transport.common.bridge.common.TransportCallback;
import com.linkedin.r2.transport.common.bridge.common.TransportResponse;

import java.util.Map;

/**
 * @author Zhenkai Zhu
 */
public class StreamDispatcherAdapter implements StreamDispatcher
{
  private final TransportDispatcher _transportDispatcher;
  private final static String CONTENT_LENGTH_HEADER = "Content-Length";

  public StreamDispatcherAdapter(TransportDispatcher transportDispatcher)
  {
    _transportDispatcher = transportDispatcher;
  }

  @Override
  public void handleStreamRequest(StreamRequest req, Map<String, String> wireAttrs,
                           RequestContext requestContext, TransportCallback<StreamResponse> callback)
  {
    Callback<ByteString> assemblyFinishCallback = getAssemblyFinishCallback(req, wireAttrs, requestContext, callback);
    Reader reader = new FullEntityReader(assemblyFinishCallback);
    req.getEntityStream().setReader(reader);
  }

  private Callback<ByteString> getAssemblyFinishCallback(final StreamRequest req, final Map<String, String> wireAttrs,
                                                         final RequestContext requestContext,
                                                         final TransportCallback<StreamResponse> callback)
  {
    return new Callback<ByteString>()
    {
      @Override
      public void onError(Throwable e)
      {
        throw new RuntimeException(e);
      }

      @Override
      public void onSuccess(ByteString result)
      {
        RestRequestBuilder builder = new RestRequestBuilder(req);
        builder.setEntity(result).setHeader(CONTENT_LENGTH_HEADER, String.valueOf(result.length()));
        _transportDispatcher.handleRestRequest(builder.build(),
            wireAttrs, requestContext, adaptToRestResponseCallback(callback));
      }
    };
  }

  private static TransportCallback<RestResponse> adaptToRestResponseCallback(final TransportCallback<StreamResponse> callback)
  {
    return new TransportCallback<RestResponse>()
    {
      @Override
      public void onResponse(final TransportResponse<RestResponse> response)
      {
        callback.onResponse(new TransportResponse<StreamResponse>()
        {
          @Override
          public StreamResponse getResponse()
          {
            // RestResponse could be used multiple times, so we should construct a StreamResponse from RestResponse
            final StreamResponse streamResponse = response.getResponse().transformBuilder()
                .build(EntityStreams.newEntityStream(new ByteStringWriter(response.getResponse().getEntity())));
            return streamResponse;
          }

          @Override
          public boolean hasError()
          {
            return response.hasError();
          }

          @Override
          public Throwable getError()
          {
            return response.getError();
          }

          @Override
          public Map<String, String> getWireAttributes()
          {
            return response.getWireAttributes();
          }
        });
      }
    };
  }
}
