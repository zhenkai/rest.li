package test.r2.perf.client;

import com.linkedin.common.callback.FutureCallback;
import com.linkedin.common.util.None;
import com.linkedin.r2.message.rest.RestRequest;
import com.linkedin.r2.message.rest.StreamRequest;
import com.linkedin.r2.transport.common.Client;
import test.r2.perf.Generator;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @auther Zhenkai Zhu
 */

public class StreamClientRunnableFactory implements ClientRunnableFactory
{
  private final Client _client;
  private final Generator<StreamRequest> _reqGen;
  private final ScheduledExecutorService _scheduler;
  private final int _qps;

  public StreamClientRunnableFactory(Client client, Generator<StreamRequest> reqGen, int qps)
  {
    _client = client;
    _reqGen = reqGen;
    _qps = qps;
    _scheduler = qps > 0? Executors.newScheduledThreadPool(24) : null;
  }

  @Override
  public Runnable create(AtomicReference<Stats> stats, CountDownLatch startLatch)
  {
    final RateLimiter rateLimiter;
    if (_qps > 0)
    {
      rateLimiter = new QpsRateLimiter(_qps, _scheduler);
      ((QpsRateLimiter)rateLimiter).init();
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
      };
    }
    return new StreamClientRunnable(_client, stats, startLatch, _reqGen, rateLimiter);
  }

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
