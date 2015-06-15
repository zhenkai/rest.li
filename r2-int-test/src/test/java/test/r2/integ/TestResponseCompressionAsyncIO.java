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

package test.r2.integ;

import com.linkedin.r2.filter.FilterChains;
import com.linkedin.r2.transport.http.server.HttpJettyServer;
import com.linkedin.r2.transport.http.server.HttpServerFactory;
import org.testng.annotations.Test;


/**
 * DISABLED UNTIL WE FIX THE BUGS.
 *
 * @author Ang Xu
 */
public class TestResponseCompressionAsyncIO // extends TestResponseCompression
{
//  protected HttpServerFactory getServerFactory()
//  {
//    return new HttpServerFactory(FilterChains.create(_compressionFilter), HttpJettyServer.ServletType.ASYNC_IO);
//  }
}
