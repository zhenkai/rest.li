package com.linkedin.r2.message.rest;

import com.linkedin.common.callback.Callback;
import com.linkedin.data.ByteString;
import com.linkedin.r2.message.streaming.ByteStringWriter;
import com.linkedin.r2.message.streaming.EntityStreams;
import com.linkedin.r2.message.streaming.FullEntityReader;


/**
 * A help class that holds static convenience methods for conversion between rest messages and stream messages
 *
 * @author Zhenkai Zhu
 */
public final class Messages
{
  private Messages() {}

  /**
   * Converts a StreamRequest to RestRequest
   * @param streamRequest the stream request to be converted
   * @param callback the callback to be invoked when the rest request is constructed
   */
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

  /**
   * Converts a StreamResponse to RestResponse
   * @param streamResponse the stream request to be converted
   * @param callback the callback to be invoked when the rest response is constructed
   */
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

  /**
   * Create a StreamRequest based on the RestRequest
   * @param restRequest the rest request
   * @return the StreamRequest that's created based on rest request
   */
  public static StreamRequest toStreamRequest(RestRequest restRequest)
  {
    StreamRequestBuilder builder = new StreamRequestBuilder(restRequest);
    return builder.build(EntityStreams.newEntityStream(new ByteStringWriter(restRequest.getEntity())));
  }

  /**
   * Create a StreamResponse based on the RestResponse
   * @param restResponse the rest response
   * @return the StreamResponse that's created based on rest response
   */
  public static StreamResponse toStreamResponse(RestResponse restResponse)
  {
    StreamResponseBuilder builder = new StreamResponseBuilder(restResponse);
    return builder.build(EntityStreams.newEntityStream(new ByteStringWriter(restResponse.getEntity())));
  }

  /**
   * Converts a StreamException to RestException
   * @param streamException the stream exception to be converted
   * @param callback the callback to be invoked when the rest exception is constructed
   */
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

  /**
   * Create a StreamException based on the RestException
   * @param restException the rest Exception
   * @return the StreamException that's created based on rest exception
   */
  public static StreamException toStreamException(final RestException restException)
  {
    return new StreamException(toStreamResponse(restException.getResponse()), restException.getMessage(), restException.getCause());
  }

  /**
   * Creates a Callback of StreamResponse based on a Callback of RestResponse.
   * If the throwable in the error case is StreamException, it will be converted to RestException.
   *
   * @param callback the callback of rest response
   * @return callback of stream response
   */
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

  /**
   * Creates a Callback of RestResponse based on a Callback of StreamResponse.
   * If the throwable in the error case is RestException, it will be converted to StreamException.
   *
   * @param callback the callback of stream response
   * @return callback of rest response
   */
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
