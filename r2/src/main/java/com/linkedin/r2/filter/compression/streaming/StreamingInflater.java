/*
   Copyright (c) 2015 LinkedIn Corp.

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

package com.linkedin.r2.filter.compression.streaming;

import com.linkedin.data.ByteString;
import com.linkedin.r2.message.streaming.WriteHandle;
import com.linkedin.r2.message.streaming.Writer;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.Executor;


/**
 * This class pipes a compressed {@link com.linkedin.r2.message.streaming.EntityStream} to
 * a different {@link com.linkedin.r2.message.streaming.EntityStream} in which the data is
 * uncompressed.
 *
 * @author Ang Xu
 */
abstract class StreamingInflater extends BufferedReaderInputStream implements Writer
{
  private static final int BUF_SIZE = 4096;

  private final Executor _executor;
  private WriteHandle _wh;
  private InputStream _in;

  public StreamingInflater(Executor executor)
  {
    _executor = executor;
  }

  @Override
  public void onInit(WriteHandle wh)
  {
    _wh = wh;
  }

  @Override
  public void onWritePossible()
  {
    _executor.execute(new Runnable()
    {
      @Override
      public void run()
      {
        try
        {
          if (_in == null)
          {
            _in = createInputStream(StreamingInflater.this);
          }

          byte[] bytes = new byte[BUF_SIZE];
          while (_wh.remaining() > 0)
          {
            int readlen = _in.read(bytes);
            if (readlen == -1)
            {
              _wh.done();
              return;
            }
            else
            {
              _wh.write(ByteString.copy(bytes, 0, readlen));
            }
          }
        }
        catch (IOException ex)
        {
          _wh.error(ex);
        }
      }
    });
  }

  @Override
  public void onAbort(Throwable e)
  {
    _wh.error(e);
    //TODO: read out remaining data from ReadHandle
  }

  abstract protected InputStream createInputStream(InputStream in) throws IOException;

}
