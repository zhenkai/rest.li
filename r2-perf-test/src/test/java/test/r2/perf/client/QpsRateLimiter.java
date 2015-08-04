package test.r2.perf.client;

import java.util.Random;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * @auther Zhenkai Zhu
 */

public class QpsRateLimiter implements RateLimiter
{
  private final long _interval;
  private final ScheduledExecutorService _scheduler;
  private final Semaphore _sem;

  public QpsRateLimiter(int qps, ScheduledExecutorService scheduler)
  {
    _interval = 1000/qps;
    _scheduler = scheduler;
    _sem = new Semaphore(0);
  }

  public void init()
  {
    long initDelay = new Random(System.currentTimeMillis()).nextLong() % _interval;
    _scheduler.scheduleAtFixedRate(new Runnable()
    {
      @Override
      public void run()
      {
        _sem.release();
      }
    }, initDelay, _interval, TimeUnit.MILLISECONDS);
  }

  @Override
  public boolean acquirePermit()
  {
    try
    {
      _sem.acquire();
      return true;
    }
    catch (InterruptedException ex)
    {
      return false;
    }
  }
}
