package com.linkedin.r2.filter.transport;

import com.linkedin.common.callback.Callback;
import com.linkedin.r2.filter.NextFilter;
import com.linkedin.r2.filter.message.rest.StreamRequestFilter;
import com.linkedin.r2.message.RequestContext;
import com.linkedin.r2.message.rest.Messages;
import com.linkedin.r2.message.rest.QueryTunnelUtil;
import com.linkedin.r2.message.rest.RestResponse;
import com.linkedin.r2.message.rest.RestStatus;
import com.linkedin.r2.message.rest.StreamRequest;
import com.linkedin.r2.message.rest.StreamResponse;

import javax.mail.MessagingException;
import java.net.URISyntaxException;
import java.util.Map;

/**
 * @author Zhenkai Zhu
 */
public class ServerQueryTunnelFilter implements StreamRequestFilter
{
  @Override
  public void onRequest(final StreamRequest req,
                            final RequestContext requestContext,
                            final Map<String, String> wireAttrs,
                            final NextFilter<StreamRequest, StreamResponse> nextFilter)
  {
    Callback<StreamRequest> callback = new Callback<StreamRequest>()
    {
      @Override
      public void onError(Throwable e)
      {
        if (e instanceof MessagingException || e instanceof URISyntaxException)
        {
          RestResponse errorResponse =
              RestStatus.responseForStatus(RestStatus.BAD_REQUEST, e.toString());
          nextFilter.onResponse(Messages.toStreamResponse(errorResponse), requestContext, wireAttrs);
        }
        else
        {
          nextFilter.onError(e, requestContext, wireAttrs);
        }
      }

      @Override
      public void onSuccess(StreamRequest newReq)
      {
        nextFilter.onRequest(newReq, requestContext, wireAttrs);
      }
    };

    QueryTunnelUtil.decode(req, requestContext, callback);
  }
}