package com.linkedin.r2.transport.common;

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


/**
 * @author Zhenkai Zhu
 */
public class StreamRequestHandlerAdapter implements StreamRequestHandler
{
  private final RestRequestHandler _restRequestHandler;
  private final static String CONTENT_LENGTH_HEADER = "Content-Length";

  public StreamRequestHandlerAdapter(RestRequestHandler restRequestHandler)
  {
    _restRequestHandler = restRequestHandler;
  }

  @Override
  public void handleRequest(StreamRequest request, RequestContext requestContext, Callback<StreamResponse> callback)
  {
    Callback<ByteString> assemblyFinishCallback = getAssemblyFinishCallback(request, requestContext, callback);
    Reader reader = new FullEntityReader(assemblyFinishCallback);
    request.getEntityStream().setReader(reader);
  }

  private Callback<ByteString> getAssemblyFinishCallback(final StreamRequest req,
                                                         final RequestContext requestContext,
                                                         final Callback<StreamResponse> callback)
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
        _restRequestHandler.handleRequest(builder.build(),
            requestContext, adaptToRestResponseCallback(callback));
      }
    };
  }

  private static Callback<RestResponse> adaptToRestResponseCallback(final Callback<StreamResponse> callback)
  {
     return new Callback<RestResponse>()
    {
      @Override
      public void onError(Throwable e)
      {
        callback.onError(e);
      }

       @Override
       public void onSuccess(RestResponse result)
       {
         // RestResponse could be used multiple times, so we should construct a StreamResponse from RestResponse
         final StreamResponse streamResponse = result.transformBuilder()
             .build(EntityStreams.newEntityStream(new ByteStringWriter(result.getEntity())));
         callback.onSuccess(streamResponse);
       }
    };
  }
}
