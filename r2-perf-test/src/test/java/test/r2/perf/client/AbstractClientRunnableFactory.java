package test.r2.perf.client;

import com.linkedin.common.callback.FutureCallback;
import com.linkedin.common.util.None;
import com.linkedin.r2.transport.common.Client;
import test.r2.perf.Generator;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @auther Zhenkai Zhu
 */

public abstract class AbstractClientRunnableFactory<T> implements ClientRunnableFactory
{
  private final Client _client;
  private final Generator<T> _reqGen;
  private final Generator<T> _warmUpReqGen;
  private final ScheduledExecutorService _scheduler;
  private final int _qps;

  public AbstractClientRunnableFactory(Client client, Generator<T> reqGen, Generator<T> warmUpReqGen, int qps)
  {
    _client = client;
    _reqGen = reqGen;
    _warmUpReqGen = warmUpReqGen;
    _qps = qps;
    _scheduler = qps > 0? Executors.newScheduledThreadPool(24) : null;
  }

  @Override
  public Runnable create(AtomicReference<Stats> stats, AtomicBoolean warmUpFinished, CountDownLatch startLatch)
  {
    final RateLimiter rateLimiter;
    if (_qps > 0)
    {
      rateLimiter = new QpsRateLimiter(_qps, _scheduler);
    }
    else
    {
      rateLimiter = new RateLimiter()
      {
        @Override
        public boolean acquirePermit()
        {
          return true;
        }

        @Override
        public void init() {}
      };
    }
    return create(_client, stats, warmUpFinished, startLatch, _reqGen, _warmUpReqGen, rateLimiter);
  }

  protected abstract Runnable create(Client client, AtomicReference<Stats> stats, AtomicBoolean warmUpFinished, CountDownLatch startLatch, Generator<T> reqGen,
                            Generator<T> warmUpReqGen, RateLimiter rateLimiter);

  @Override
  public void shutdown()
  {
    final FutureCallback<None> callback = new FutureCallback<None>();
    _client.shutdown(callback);

    try
    {
      callback.get();
    }
    catch (Exception e)
    {
      // Print out error and continue
      e.printStackTrace();
    }
  }
}
