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
package test.r2.perf.driver;

import test.r2.perf.PerfConfig;
import test.r2.perf.client.PerfClient;
import test.r2.perf.client.PerfClients;

import java.net.URI;

/**
 * @author Chris Pettitt
 * @version $Revision$
 */
public class RunHttpRestClient
{
  public static void main(String[] args) throws Exception
  {
    final URI uri = PerfConfig.getHttpUri();
    final int numThreads = PerfConfig.getNumClientThreads();
    final int numMsgs = PerfConfig.getNumMessages();
    final int msgSize = PerfConfig.getMessageSize();
    final boolean pureStreaming = PerfConfig.isClientPureStreaming();
    final int qps = PerfConfig.getQpsPerThread();
    final int warmUpMs = PerfConfig.getPerfWarmUPMs();

    final PerfClient client;

    client = PerfClients.httpRest(uri, numThreads, numMsgs, msgSize, qps, warmUpMs);
    client.run();
    client.shutdown();
  }
}
