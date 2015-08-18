package test.r2.perf.client;

import com.linkedin.r2.message.rest.StreamRequest;
import com.linkedin.r2.transport.common.Client;
import test.r2.perf.Generator;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @auther Zhenkai Zhu
 */

public class StreamClientRunnableFactory extends AbstractClientRunnableFactory<StreamRequest>
{
  public StreamClientRunnableFactory(Client client, Generator<StreamRequest> reqGen, Generator<StreamRequest> warmUpReqGen, int qps)
  {
    super(client, reqGen, warmUpReqGen, qps);
  }

  @Override
  protected Runnable create(Client client, AtomicReference<Stats> stats, AtomicBoolean warmUpFinished, CountDownLatch startLatch, Generator<StreamRequest> reqGen,
                            Generator<StreamRequest> warmUpReqGen, RateLimiter rateLimiter)
  {
    return new StreamClientRunnable(client, stats, startLatch, warmUpFinished, reqGen, warmUpReqGen, rateLimiter);
  }
}