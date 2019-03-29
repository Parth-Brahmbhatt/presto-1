/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.prestosql.server;

import io.airlift.log.Logger;

import java.io.File;
import java.io.FileOutputStream;

public class NetflixShutdownAction
        implements ShutdownAction
{
    private static final Logger log = Logger.get(NetflixShutdownAction.class);

    @Override
    public void onShutdown()
    {
        File file = new File("/logs/presto/down");
        if (!file.exists()) {
            try {
                new FileOutputStream(file).close();
            }
            catch (Exception e) {
                // Do nothing
            }
        }
        System.exit(0);
    }
}
