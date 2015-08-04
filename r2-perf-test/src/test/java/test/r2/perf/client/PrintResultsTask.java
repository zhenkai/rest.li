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

import com.linkedin.common.stats.LongStats;
import test.r2.perf.PerfConfig;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Chris Pettitt
 * @version $Revision$
 */
public class PrintResultsTask implements Runnable
{
  private final AtomicReference<Stats> _statsRef;

  public PrintResultsTask(AtomicReference<Stats> statsRef)
  {
    _statsRef = statsRef;
  }

  public void run()
  {
    final Stats stats = _statsRef.get();
    LongStats snapshot = stats.getLatencyStats();
    long elapsedTime = stats.getElapsedTime();
    double timePerReq = stats.getSuccessCount() != 0 ? elapsedTime/(double)stats.getSuccessCount() : 0;
    double reqPerSec = timePerReq != 0 ? 1000.0 / timePerReq : 0;

    StringBuilder sb = new StringBuilder();
    sb.append("\n");
    sb.append("\n");
    sb.append("DONE\n");
    sb.append("\n");
    sb.append("Results\n");
    sb.append("-------\n");
    sb.append("    Total Requests: " + stats.getSentCount() + "\n");
    sb.append("    Elapsed: " + elapsedTime + "\n");
    sb.append("    Mean latency (in millis): " + snapshot.getAverage() / 10E6 + "\n");
    sb.append("    Reqs / Sec: " + reqPerSec + "\n");
    sb.append("    Errors: " + stats.getErrorCount() + "\n");
    sb.append("    Min latency: " + snapshot.getMinimum() / 10E6 + "\n");
    sb.append("    50% latency: " + snapshot.get50Pct() / 10E6 + "\n");
    sb.append("    90% latency: " + snapshot.get90Pct() / 10E6 + "\n");
    sb.append("    95% latency: " + snapshot.get95Pct() / 10E6 + "\n");
    sb.append("    99% latency: " + snapshot.get99Pct() / 10E6 + "\n");
    sb.append("    Max latency: " + snapshot.getMaximum() / 10E6 + "\n");

    String result = sb.toString();
    System.out.print(result);

    try
    {
      File file = new File(PerfConfig.getOutputDir() + "/result.output");
      if (!file.exists())
      {
        file.createNewFile();
      }
      FileOutputStream os = new FileOutputStream(file, false);
      PrintWriter printWriter = new PrintWriter(os);
      printWriter.print(result);
      printWriter.close();
    }
    catch (IOException e)
    {
      e.printStackTrace();
    }

  }
}
