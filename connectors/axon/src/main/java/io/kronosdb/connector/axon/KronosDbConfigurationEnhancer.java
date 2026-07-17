package io.kronosdb.connector.axon;

import io.kronosdb.connector.axon.command.KronosDbCommandBusConnector;
import io.kronosdb.connector.axon.event.EventProcessorControlService;
import io.kronosdb.connector.axon.event.KronosDbEventStorageEngine;
import io.kronosdb.connector.axon.query.KronosDbQueryBusConnector;
import io.kronosdb.connector.axon.snapshot.KronosDbSnapshotStore;
import org.axonframework.common.configuration.ApplicationConfigurer;
import org.axonframework.common.configuration.ComponentDecorator;
import org.axonframework.common.configuration.ComponentDefinition;
import org.axonframework.common.configuration.ComponentLifecycleHandler;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.common.configuration.ConfigurationEnhancer;
import org.axonframework.common.configuration.SearchScope;
import org.axonframework.common.lifecycle.Phase;
import org.axonframework.conversion.Converter;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.snapshot.store.SnapshotStore;
import org.axonframework.messaging.commandhandling.distributed.CommandBusConnector;
import org.axonframework.messaging.commandhandling.distributed.PayloadConvertingCommandBusConnector;
import org.axonframework.messaging.core.conversion.MessageConverter;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import org.axonframework.messaging.queryhandling.distributed.PayloadConvertingQueryBusConnector;
import org.axonframework.messaging.queryhandling.distributed.QueryBusConnector;

/**
 * A {@link ConfigurationEnhancer} that is auto-loadable by the {@link ApplicationConfigurer},
 * setting sensible defaults when using KronosDB.
 * <p>
 * When this JAR is on the classpath, Axon Framework will automatically discover this enhancer
 * via ServiceLoader and wire up the KronosDB-backed event store, snapshot store,
 * command bus connector, and query bus connector.
 */
public class KronosDbConfigurationEnhancer implements ConfigurationEnhancer {

    public static final int ENHANCER_ORDER = Integer.MIN_VALUE + 10;

    @Override
    public void enhance(ComponentRegistry registry) {
        registry.registerIfNotPresent(KronosDbConfiguration.class,
                        c -> new KronosDbConfiguration(),
                        SearchScope.ALL)
                .registerIfNotPresent(connectionManagerDefinition(), SearchScope.ALL)
                .registerIfNotPresent(eventStorageEngineDefinition(), SearchScope.ALL)
                .registerIfNotPresent(commandBusConnectorDefinition(), SearchScope.ALL)
                .registerIfNotPresent(queryBusConnectorDefinition(), SearchScope.ALL)
                .registerDecorator(CommandBusConnector.class,
                        0,
                        payloadConvertingCommandConnector())
                .registerDecorator(QueryBusConnector.class,
                        0,
                        payloadConvertingQueryConnector())
                .registerIfNotPresent(snapshotStoreDefinition())
                .registerIfNotPresent(eventProcessorControlServiceDefinition());
    }

    private static ComponentDefinition<KronosDbConnectionManager> connectionManagerDefinition() {
        return ComponentDefinition.ofType(KronosDbConnectionManager.class)
                .withBuilder(KronosDbConfigurationEnhancer::buildConnectionManager)
                .onStart(Phase.INSTRUCTION_COMPONENTS, KronosDbConnectionManager::start)
                .onShutdown(Phase.EXTERNAL_CONNECTIONS, KronosDbConnectionManager::shutdown);
    }

    private static KronosDbConnectionManager buildConnectionManager(Configuration config) {
        KronosDbConfiguration kronosConfig = config.getComponent(KronosDbConfiguration.class);
        return KronosDbConnectionManager.builder()
                .configuration(kronosConfig)
                .build();
    }

    private static ComponentDefinition<EventStorageEngine> eventStorageEngineDefinition() {
        return ComponentDefinition.ofType(EventStorageEngine.class)
                .withBuilder(config -> new KronosDbEventStorageEngine(
                        config.getComponent(KronosDbConnectionManager.class).getConnection(),
                        config.getComponent(EventConverter.class)
                ));
    }

    private static ComponentDefinition<CommandBusConnector> commandBusConnectorDefinition() {
        return ComponentDefinition.ofType(CommandBusConnector.class)
                .withBuilder(config -> new KronosDbCommandBusConnector(
                        config.getComponent(KronosDbConnectionManager.class).getConnection(),
                        config.getComponent(KronosDbConfiguration.class),
                        config.getComponent(MessageConverter.class)
                ))
                .onStart(Phase.INBOUND_COMMAND_CONNECTOR,
                        connector -> ((KronosDbCommandBusConnector) connector).start())
                .onShutdown(Phase.INBOUND_COMMAND_CONNECTOR,
                        (ComponentLifecycleHandler<CommandBusConnector>) (config, connector) ->
                                ((KronosDbCommandBusConnector) connector).disconnect())
                .onShutdown(Phase.OUTBOUND_COMMAND_CONNECTORS,
                        (ComponentLifecycleHandler<CommandBusConnector>) (config, connector) ->
                                ((KronosDbCommandBusConnector) connector).shutdownDispatching());
    }

    private static ComponentDefinition<QueryBusConnector> queryBusConnectorDefinition() {
        return ComponentDefinition.ofType(QueryBusConnector.class)
                .withBuilder(config -> new KronosDbQueryBusConnector(
                        config.getComponent(KronosDbConnectionManager.class).getConnection(),
                        config.getComponent(KronosDbConfiguration.class),
                        config.getComponent(MessageConverter.class)
                ))
                .onStart(Phase.INBOUND_QUERY_CONNECTOR,
                        connector -> ((KronosDbQueryBusConnector) connector).start())
                .onShutdown(Phase.INBOUND_QUERY_CONNECTOR,
                        (ComponentLifecycleHandler<QueryBusConnector>) (config, connector) ->
                                ((KronosDbQueryBusConnector) connector).disconnect())
                .onShutdown(Phase.OUTBOUND_QUERY_CONNECTORS,
                        (ComponentLifecycleHandler<QueryBusConnector>) (config, connector) ->
                                ((KronosDbQueryBusConnector) connector).shutdownDispatching());
    }

    private static ComponentDecorator<CommandBusConnector, PayloadConvertingCommandBusConnector> payloadConvertingCommandConnector() {
        return (config, name, delegate) -> new PayloadConvertingCommandBusConnector(
                delegate,
                config.getComponent(MessageConverter.class),
                byte[].class
        );
    }

    private static ComponentDecorator<QueryBusConnector, PayloadConvertingQueryBusConnector> payloadConvertingQueryConnector() {
        return (config, name, delegate) -> new PayloadConvertingQueryBusConnector(
                delegate,
                config.getComponent(MessageConverter.class),
                byte[].class
        );
    }

    private static ComponentDefinition<SnapshotStore> snapshotStoreDefinition() {
        return ComponentDefinition.ofType(SnapshotStore.class)
                .withBuilder(c -> new KronosDbSnapshotStore(
                        c.getComponent(KronosDbConnectionManager.class).getConnection(),
                        c.getComponent(Converter.class)
                ));
    }

    private static ComponentDefinition<EventProcessorControlService> eventProcessorControlServiceDefinition() {
        return ComponentDefinition.ofType(EventProcessorControlService.class)
                .withBuilder(config -> new EventProcessorControlService(
                        config,
                        config.getComponent(KronosDbConnectionManager.class)
                                .getConnection()
                                .platformChannel()
                ))
                .onStart(Phase.INSTRUCTION_COMPONENTS, EventProcessorControlService::start)
                .onShutdown(Phase.INSTRUCTION_COMPONENTS, EventProcessorControlService::shutdown);
    }

    @Override
    public int order() {
        return ENHANCER_ORDER;
    }
}
