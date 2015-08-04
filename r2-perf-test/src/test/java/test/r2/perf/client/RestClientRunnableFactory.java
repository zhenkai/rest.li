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

import com.linkedin.r2.message.rest.RestRequest;
import com.linkedin.r2.transport.common.Client;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import test.r2.perf.Generator;


/**
 * @author Chris Pettitt
 * @version $Revision$
 */
public class RestClientRunnableFactory extends AbstractClientRunnableFactory<RestRequest>
{
  public RestClientRunnableFactory(Client client, Generator<RestRequest> reqGen, Generator<RestRequest> warmUpReqGen, int qps)
  {
    super(client, reqGen, warmUpReqGen, qps);
  }

  @Override
  protected Runnable create(Client client, AtomicReference<Stats> stats, AtomicBoolean warmUpFinished, CountDownLatch startLatch, Generator<RestRequest> reqGen,
                                     Generator<RestRequest> warmUpReqGen, RateLimiter rateLimiter)
  {
    return new RestClientRunnable(client, stats, startLatch, warmUpFinished, reqGen, warmUpReqGen, rateLimiter);
  }
}
