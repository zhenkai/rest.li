package com.linkedin.r2.message.rest;

import com.linkedin.common.callback.Callback;
import com.linkedin.data.ByteString;
import com.linkedin.r2.message.streaming.ByteStringWriter;
import com.linkedin.r2.message.streaming.EntityStreams;
import com.linkedin.r2.message.streaming.FullEntityReader;
import com.linkedin.r2.message.streaming.Reader;


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

  public static void toRestException(final StreamException streamException, final Callback<RestException> callback)
  {
    toRestResponse(streamException.getResponse(), new Callback<RestResponse>()
    {
      @Override
      public void onError(Throwable e)
      {
        callback.onError(e);
      }

      @Override
      public void onSuccess(RestResponse result)
      {
        callback.onSuccess(new RestException(result, streamException.getMessage(), streamException.getCause()));

      }
    });
  }

  public static StreamException toStreamException(final RestException restException)
  {
    return new StreamException(toStreamResponse(restException.getResponse()), restException.getMessage(), restException.getCause());
  }

  public static Callback<StreamResponse> toStreamCallback(final Callback<RestResponse> callback)
  {
    return new Callback<StreamResponse>()
    {
      @Override
      public void onError(Throwable originalException)
      {
        if (originalException instanceof StreamException)
        {
          toRestException((StreamException)originalException, new Callback<RestException>()
          {
            @Override
            public void onError(Throwable e)
            {
              // shall we use originalException?
              callback.onError(e);
            }

            @Override
            public void onSuccess(RestException restException)
            {
              callback.onError(restException);
            }
          });
        }
        else
        {
          callback.onError(originalException);
        }
      }

      @Override
      public void onSuccess(StreamResponse streamResponse)
      {
        toRestResponse(streamResponse, new Callback<RestResponse>()
        {
          @Override
          public void onError(Throwable e)
          {
            callback.onError(e);
          }

          @Override
          public void onSuccess(RestResponse restResponse)
          {
            callback.onSuccess(restResponse);
          }
        });
      }
    };
  }

  public static Callback<RestResponse> toRestCallback(final Callback<StreamResponse> callback)
  {
    return new Callback<RestResponse>()
    {
      @Override
      public void onError(Throwable e)
      {
        if (e instanceof RestException)
        {
          callback.onError(toStreamException((RestException)e));
        }
        else
        {
          callback.onError(e);
        }
      }

      @Override
      public void onSuccess(RestResponse result)
      {
        callback.onSuccess(toStreamResponse(result));
      }
    };
  }

}
