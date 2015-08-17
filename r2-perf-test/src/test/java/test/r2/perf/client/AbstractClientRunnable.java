/*
   Copyright (c) 2012 LinkedIn Corp.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

/* $Id$ */
package test.r2.perf.client;

import com.linkedin.common.callback.Callback;
import com.linkedin.common.callback.FutureCallback;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import test.r2.perf.Generator;


/**
 * @author Chris Pettitt
 * @version $Revision$
 */
/* package private */ abstract class AbstractClientRunnable<REQ, RES> implements Runnable
{
  private final AtomicReference<Stats> _stats;
  private final CountDownLatch _startLatch;
  private final Generator<REQ> _workGen;
  private final Generator<REQ> _warmUpReqGen;
  private final RateLimiter _rateLimiter;
  private final AtomicBoolean _warmUpFinished;

  public AbstractClientRunnable(AtomicReference<Stats> stats,
                                AtomicBoolean warmUpFinished,
                                CountDownLatch startLatch,
                                Generator<REQ> reqGen,
                                Generator<REQ> warmUpReqGen)
  {
    this(stats, warmUpFinished, startLatch, reqGen, warmUpReqGen, null);
  }

  public AbstractClientRunnable(AtomicReference<Stats> stats,
                                AtomicBoolean warmUpFinished,
                                CountDownLatch startLatch,
                                Generator<REQ> reqGen,
                                Generator<REQ> warmUpReqGen,
                                RateLimiter rateLimiter)
  {
    _stats = stats;
    _warmUpFinished = warmUpFinished;
    _startLatch = startLatch;
    _workGen = reqGen;
    _warmUpReqGen = warmUpReqGen;
    _rateLimiter = rateLimiter;
  }

  @Override
  public void run()
  {
    if (_rateLimiter != null)
    {
      _rateLimiter.init();
    }

    warmUp();

    REQ nextMsg;

    while ((nextMsg = _workGen.nextMessage()) != null)
    {
      if (_rateLimiter != null)
      {
        _rateLimiter.acquirePermit();
        asyncSendMessageWithStats(nextMsg);
      }
      else
      {
        sendMessageWithStats(nextMsg);
      }
    }
  }

  private void warmUp()
  {
    REQ warmUpNextMsg;
    while((warmUpNextMsg = _warmUpReqGen.nextMessage()) != null && !_warmUpFinished.get())
    {
      if (_rateLimiter != null)
      {
        _rateLimiter.acquirePermit();
        asyncSendMessageWithStats(warmUpNextMsg);
      }
      else
      {
        sendMessageWithStats(warmUpNextMsg);
      }
    }

    try
    {
      _startLatch.await();
    }
    catch (InterruptedException e)
    {
      e.printStackTrace();
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  private void sendMessageWithStats(REQ msg)
  {
    final FutureCallback<RES> callback = new FutureCallback<RES>();

    long start = System.nanoTime();

    sendMessage(msg, callback);

    final Stats stats = _stats.get();

    stats.sent();

    try
    {
      callback.get();
      long elapsed = System.nanoTime() - start;
      stats.success(elapsed);
    }
    catch (Exception e)
    {
      stats.error(e);
    }
  }

  private void asyncSendMessageWithStats(REQ msg)
  {
    final Stats stats = _stats.get();

    final long start = System.nanoTime();

    final Callback<RES> timingCallback = new Callback<RES>()
    {
      @Override
      public void onError(Throwable e)
      {
        stats.error((Exception)e);
      }

      @Override
      public void onSuccess(RES result)
      {
        long elapsed = System.nanoTime() - start;
        stats.success(elapsed);
      }
    };

    sendMessage(msg, timingCallback);
    stats.sent();
  }

  protected abstract void sendMessage(REQ nextMsg, Callback<RES> callback);

}
