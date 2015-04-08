package com.linkedin.r2.message.rest;

import com.linkedin.common.callback.Callback;
import com.linkedin.data.ByteString;
import com.linkedin.r2.message.streaming.ByteStringWriter;
import com.linkedin.r2.message.streaming.EntityStreams;
import com.linkedin.r2.message.streaming.FullEntityReader;


/**
 * @author Zhenkai Zhu
 */
public final class Messages
{
  private Messages() {}

  public static void toRestRequest(StreamRequest streamRequest, final Callback<RestRequest> callback)
  {
    final RestRequestBuilder builder = new RestRequestBuilder(streamRequest);
    Callback<ByteString> assemblyCallback = new Callback<ByteString>()
    {
      @Override
      public void onError(Throwable e)
      {
        callback.onError(e);
      }

      @Override
      public void onSuccess(ByteString result)
      {
        RestRequest restRequest = builder.setEntity(result).build();
        callback.onSuccess(restRequest);
      }
    };
    streamRequest.getEntityStream().setReader(new FullEntityReader(assemblyCallback));
  }

  public static void toRestResponse(StreamResponse streamResponse, final Callback<RestResponse> callback)
  {
    final RestResponseBuilder builder = new RestResponseBuilder(streamResponse);
    Callback<ByteString> assemblyCallback = new Callback<ByteString>()
    {
      @Override
      public void onError(Throwable e)
      {
        callback.onError(e);
      }

      @Override
      public void onSuccess(ByteString result)
      {
        RestResponse restResponse = builder.setEntity(result).build();
        callback.onSuccess(restResponse);
      }
    };
    streamResponse.getEntityStream().setReader(new FullEntityReader(assemblyCallback));
  }

  public static StreamRequest toStreamRequest(RestRequest restRequest)
  {
    StreamRequestBuilder builder = new StreamRequestBuilder(restRequest);
    return builder.build(EntityStreams.newEntityStream(new ByteStringWriter(restRequest.getEntity())));
  }

  public static StreamResponse toStreamResponse(RestResponse restResponse)
  {
    StreamResponseBuilder builder = new StreamResponseBuilder(restResponse);
    return builder.build(EntityStreams.newEntityStream(new ByteStringWriter(restResponse.getEntity())));
  }
}
