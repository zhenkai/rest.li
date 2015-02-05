package com.linkedin.r2.util;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * This class ensures the events with the same key are always executed on the same thread in order.
 *
 * The basic idea is that we create N single thread executors, and assign the Event with key K to the
 * (K % N)th executor.
 *
 * @author Zhenkai Zhu
 */
public class StickyEventExecutor
{
  final List<ExecutorService> _executors;
  final long _shutdownWaitMillis;
  final private static Logger LOG = LoggerFactory.getLogger(StickyEventExecutor.class);

  public StickyEventExecutor(String name, int threadNum, long shutdownWaitMillis)
  {
    List<ExecutorService> executors = new ArrayList<ExecutorService>(threadNum);
    for (int i = 0; i < threadNum; i++)
    {
      executors.add(Executors.newSingleThreadExecutor(new NamedThreadFactory(name)));
    }
    _executors = Collections.unmodifiableList(executors);
    _shutdownWaitMillis = shutdownWaitMillis;
  }

  public void dispatch(Event event)
  {
    int index = event.key() % _executors.size();
    ExecutorService executor = _executors.get(index);
    executor.submit(event);
  }

  public void shutdown()
  {
    for (ExecutorService executor: _executors)
    {
      executor.shutdown();
    }

    long maxWaitTime = _shutdownWaitMillis;

    try
    {
      for (ExecutorService executor : _executors)
      {
        long startTime = System.nanoTime();
        if (maxWaitTime < 0 || !executor.awaitTermination(maxWaitTime, TimeUnit.MILLISECONDS))
        {
          LOG.warn(StickyEventExecutor.class.getName() + ".shutdown didn't finish in time. Continuing...");
          break;
        }
        long timespan = (System.nanoTime() - startTime) / 1000000;
        maxWaitTime -= timespan;
      }
    }
    catch (InterruptedException ex)
    {
      throw new RuntimeException(ex);
    }
  }

  /**
   * A wrapper class for Runnable that has a key.
   */
  public static class Event implements Runnable
  {
    final private int _key;
    final private Runnable _runnable;

    public Event(int key, Runnable runnable)
    {
      _key = key;
      _runnable = runnable;
    }

    public int key()
    {
      return _key;
    }

    @Override
    public void run()
    {
      _runnable.run();
    }
  }
}
