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
package io.prestosql.plugin.arrowflight;

import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.prestosql.spi.connector.Connector;
import io.prestosql.spi.connector.ConnectorContext;
import io.prestosql.spi.connector.ConnectorFactory;
import io.prestosql.spi.connector.ConnectorHandleResolver;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class ArrowFlightConnectorFactory
        implements ConnectorFactory
{
    @Override
    public String getName()
    {
        return "arrow-flight";
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return new ArrowFlightHandleResolver();
    }

    @Override
    public Connector create(String catalogName, Map<String, String> requiredConfig, ConnectorContext context)
    {
        requireNonNull(requiredConfig, "requiredConfig is null");

        // A plugin is not required to use Guice; it is just very convenient
        Bootstrap app = new Bootstrap(
                // new JsonModule(),
                new ArrowFlightModule(context.getTypeManager()));

        Injector injector = app
                .strictConfig()
                .doNotInitializeLogging()
                .setRequiredConfigurationProperties(requiredConfig)
                .initialize();

        return injector.getInstance(ArrowFlightConnector.class);
    }
}
