package com.memsql.talend.components;

import java.lang.reflect.Field;
import java.sql.SQLException;
import java.util.Properties;


import com.memsql.talend.components.dataset.SQLQueryDataset;
import com.memsql.talend.components.dataset.TableNameDataset;
import com.memsql.talend.components.datastore.MemSQLDatastore;
import com.memsql.talend.components.output.Output;
import com.memsql.talend.components.output.OutputConfiguration;
import com.memsql.talend.components.service.I18nMessage;
import com.memsql.talend.components.service.MemsqlComponentService;
import com.memsql.talend.components.source.SQLQueryInputMapperConfiguration;
import com.memsql.talend.components.source.SQLQueryInputSource;
import com.memsql.talend.components.source.TableNameInputMapperConfiguration;
import com.memsql.talend.components.source.TableNameInputSource;
import org.talend.sdk.component.junit.environment.EnvironmentConfiguration.Property;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.bootstrap.HttpServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.junit.BaseComponentsHandler;
import org.talend.sdk.component.junit.environment.Environment;
import org.talend.sdk.component.junit.environment.EnvironmentConfiguration;
import org.talend.sdk.component.junit.environment.builtin.ContextualEnvironment;
import org.talend.sdk.component.junit.environment.builtin.beam.SparkRunnerEnvironment;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.junit5.WithMavenServers;
import org.talend.sdk.component.junit5.environment.EnvironmentalTest;
import org.talend.sdk.component.runtime.record.RecordBuilderFactoryImpl;

@Environment(ContextualEnvironment.class)
@EnvironmentConfiguration(environment = "Contextual", systemProperties = {}) // EnvironmentConfiguration is necessary for each
                                                                             // @Environment
@Environment(SparkRunnerEnvironment.class)
@EnvironmentConfiguration(environment = "Spark", systemProperties = {
        @Property(key = "talend.beam.job.runner", value = "org.apache.beam.runners.spark.SparkRunner"),
        @Property(key = "talend.beam.job.filesToStage", value = ""), @Property(key = "spark.ui.enabled", value = "false") })

@WithMavenServers
@WithComponents("com.memsql.talend.components")
@TestMethodOrder(OrderAnnotation.class)
public class MemSQLTests extends MemSQLBaseTest {   

    @Service
    private MemsqlComponentService service;

    private MemSQLDatastore datastore;

    private Properties props;


    @BeforeAll
    public static void initialize()
    {
        String[] args = {"start"};
        MemSQLDocker.main(args);
        
    }

    @AfterAll
    public static void deinitialize()
    {
        String[] args = {"stop"};
        MemSQLDocker.main(args);
    }

    @BeforeEach
    public void setup()
    {
        props = memsqlProps();
        datastore = buildDataStore(props);
    }

    @EnvironmentalTest
    @Order(1)
    public void validateConnection() throws NoSuchFieldException, IllegalAccessException {
        
        final HealthCheckStatus status = service.validateConnection(datastore);
        Assertions.assertNotNull(status);
        Assertions.assertEquals(HealthCheckStatus.Status.OK, status.getStatus(),"MemSQLDatastore Connection Valid");

    }

    /*
    @EnvironmentalTest
    @Order(2)
    public void tableNameDatasetTest() {
        Properties props = memsqlProps();
        MemSQLDatastore datastore = buildDataStore(props);
        TableNameDataset dataset = new TableNameDataset();
        dataset.setDatastore(datastore);
        dataset.setFetchSize(1000);
        dataset.setTableName(props.getProperty("memsql.table"));

        Assertions.assertEquals("select * from " + props.getProperty("memsql.table"),dataset.getQuery());
    }

    @EnvironmentalTest
    @Order(3)
    public void tableNameInputSourceTest() throws NoSuchFieldException, IllegalAccessException {
            Properties props = memsqlProps();
            MemSQLDatastore datastore = buildDataStore(props);
            TableNameDataset dataset = new TableNameDataset();
            dataset.setDatastore(datastore);
            dataset.setFetchSize(1000);
            dataset.setTableName(props.getProperty("memsql.table"));
            
            I18nMessage i18n = getI18n();
            final Field internationalField = MemsqlComponentService.class.getDeclaredField("i18n");
            internationalField.setAccessible(true);
            internationalField.set(service, i18n);
            TableNameInputMapperConfiguration configuration = new TableNameInputMapperConfiguration();
            configuration.setDataset(dataset);
            RecordBuilderFactory factory = new RecordBuilderFactoryImpl(null);
            TableNameInputSource source = new TableNameInputSource(configuration, service, factory , i18n);
            source.init();
            Record record = source.next();
            Assertions.assertNotNull(record);
            source.release();
    }

    @EnvironmentalTest
    @Order(4)
    public void sqlQueryDatasetTest()
    {
        Properties props = memsqlProps();
        MemSQLDatastore datastore = buildDataStore(props);
        SQLQueryDataset dataset = new SQLQueryDataset();
        dataset.setDatastore(datastore);
        dataset.setFetchSize(1000);
        dataset.setSqlQuery("select * from " + props.getProperty("memsql.table"));

        Assertions.assertEquals("select * from " + props.getProperty("memsql.table"),dataset.getQuery());
    }

    @EnvironmentalTest
    @Order(5)
    public void sqlQueryInputSourceTest() throws NoSuchFieldException, IllegalAccessException
    {

            Properties props = memsqlProps();
            MemSQLDatastore datastore = buildDataStore(props);
            SQLQueryDataset dataset = new SQLQueryDataset();
            dataset.setDatastore(datastore);
            dataset.setFetchSize(1000);
            dataset.setSqlQuery("select * from " + props.getProperty("memsql.table"));
            
            I18nMessage i18n = getI18n();
            final Field internationalField = MemsqlComponentService.class.getDeclaredField("i18n");
            internationalField.setAccessible(true);
            internationalField.set(service, i18n);
            SQLQueryInputMapperConfiguration configuration = new SQLQueryInputMapperConfiguration();
            configuration.setDataset(dataset);
            RecordBuilderFactory factory = new RecordBuilderFactoryImpl(null);
            SQLQueryInputSource source = new SQLQueryInputSource(configuration, service, factory , i18n);
            source.init();
            Record record = source.next();
            Assertions.assertNotNull(record);
            source.release();

    }

    @EnvironmentalTest
    @Order(6)
    public void outputInsertTest() throws NoSuchFieldException, IllegalAccessException, SQLException
    {

            Properties props = memsqlProps();
            MemSQLDatastore datastore = buildDataStore(props);
            // Output table
            TableNameDataset dataset = new TableNameDataset();
            dataset.setDatastore(datastore);
            dataset.setFetchSize(1000);
            dataset.setTableName(props.getProperty("memsql.insert.table"));
            // Input table
            TableNameDataset input = new TableNameDataset();
            input.setDatastore(datastore);
            input.setFetchSize(1000);
            input.setTableName(props.getProperty("memsql.table"));
            
            I18nMessage i18n = getI18n();
            final Field internationalField = MemsqlComponentService.class.getDeclaredField("i18n");
            internationalField.setAccessible(true);
            internationalField.set(service, i18n);
            OutputConfiguration outputConfiguration = new OutputConfiguration();
            outputConfiguration.setActionOnData("INSERT");
            outputConfiguration.setCreateTableIfNotExists(true);
            outputConfiguration.setDataset(dataset);
            outputConfiguration.setRewriteBatchedStatements(true);
            Output output = new Output(outputConfiguration, service, i18n);
            // Read input table
            TableNameInputMapperConfiguration configuration = new TableNameInputMapperConfiguration();
            configuration.setDataset(input);
            RecordBuilderFactory factory = new RecordBuilderFactoryImpl(null);
            TableNameInputSource source = new TableNameInputSource(configuration, service, factory , i18n);
            source.init();
            Record record;
            output.beforeGroup();
            while ((record = source.next()) != null)
            {
                output.onNext(record);
            }
            output.afterGroup();
            source.release();
            output.release();

            Assertions.assertEquals(true, true, "outputInsert Success!");


    }

    @EnvironmentalTest
    @Order(7)
    public void outputBulkTest() throws NoSuchFieldException, IllegalAccessException, SQLException
    {

            Properties props = memsqlProps();
            MemSQLDatastore datastore = buildDataStore(props);
            // Output table
            TableNameDataset dataset = new TableNameDataset();
            dataset.setDatastore(datastore);
            dataset.setFetchSize(1000);
            dataset.setTableName(props.getProperty("memsql.bulk.table"));
            // Input table
            TableNameDataset input = new TableNameDataset();
            input.setDatastore(datastore);
            input.setFetchSize(1000);
            input.setTableName(props.getProperty("memsql.table"));
           
            I18nMessage i18n = getI18n();
            final Field internationalField = MemsqlComponentService.class.getDeclaredField("i18n");
            internationalField.setAccessible(true);
            internationalField.set(service, i18n);
            OutputConfiguration outputConfiguration = new OutputConfiguration();
            outputConfiguration.setActionOnData("BULK_LOAD");
            outputConfiguration.setCreateTableIfNotExists(true);
            outputConfiguration.setDataset(dataset);
            outputConfiguration.setRewriteBatchedStatements(true);
            Output output = new Output(outputConfiguration, service, i18n);
            // Read input table
            TableNameInputMapperConfiguration configuration = new TableNameInputMapperConfiguration();
            configuration.setDataset(input);
            RecordBuilderFactory factory = new RecordBuilderFactoryImpl(null);
            TableNameInputSource source = new TableNameInputSource(configuration, service, factory , i18n);
            source.init();
            Record record;
            output.beforeGroup();
            while ((record = source.next()) != null)
            {
                output.onNext(record);
            }
            output.afterGroup();
            source.release();
            output.release();

            Assertions.assertEquals(true, true, "outputInsert Success!");

    }
    */
}