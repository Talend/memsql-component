package com.memsql.talend.components;

import java.lang.reflect.Field;
import java.util.Properties;

import javax.annotation.PostConstruct;

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
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.junit.environment.Environment;
import org.talend.sdk.component.junit.environment.EnvironmentConfiguration;
import org.talend.sdk.component.junit.environment.builtin.ContextualEnvironment;
import org.talend.sdk.component.junit.environment.builtin.beam.SparkRunnerEnvironment;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.runtime.record.RecordBuilderFactoryImpl;

@Environment(ContextualEnvironment.class)
@EnvironmentConfiguration(environment = "Contextual", systemProperties = {}) // EnvironmentConfiguration is necessary for each
                                                                             // @Environment
@Environment(SparkRunnerEnvironment.class)
@EnvironmentConfiguration(environment = "Spark", systemProperties = {
        @Property(key = "talend.beam.job.runner", value = "org.apache.beam.runners.spark.SparkRunner"),
        @Property(key = "talend.beam.job.filesToStage", value = ""), @Property(key = "spark.ui.enabled", value = "false") })

@WithComponents(value = "com.memsql.talend.components")
public class MemSQLTests extends MemSQLBaseTest {   



    @Test
    public void validateConnection() {
        Properties props = memsqlProps();
        MemSQLDatastore datastore = buildDataStore(props);
        MemsqlComponentService service = new MemsqlComponentService();
        I18nMessage i18n = getI18n();
        try {
            final Field internationalField = MemsqlComponentService.class.getDeclaredField("i18n");
            internationalField.setAccessible(true);
            internationalField.set(service, i18n);
        
            HealthCheckStatus status = service.validateConnection(datastore);
            Assertions.assertEquals(HealthCheckStatus.Status.OK, status.getStatus(),"MemSQLDatastore Connection Valid");

        } catch(Exception e) {
            Assertions.assertEquals(true, false, e.getMessage());
        }
    }

    @Test
    public void tableNameDatasetTest() {
        Properties props = memsqlProps();
        MemSQLDatastore datastore = buildDataStore(props);
        TableNameDataset dataset = new TableNameDataset();
        dataset.setDatastore(datastore);
        dataset.setFetchSize(1000);
        dataset.setTableName(props.getProperty("memsql.table"));

        Assertions.assertEquals("select * from " + props.getProperty("memsql.table"),dataset.getQuery());
    }

    @Test
    public void tableNameInputSourceTest() {
        try {
            Properties props = memsqlProps();
            MemSQLDatastore datastore = buildDataStore(props);
            TableNameDataset dataset = new TableNameDataset();
            dataset.setDatastore(datastore);
            dataset.setFetchSize(1000);
            dataset.setTableName(props.getProperty("memsql.table"));
            MemsqlComponentService service = new MemsqlComponentService();
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
        } catch(Exception e) {
            Assertions.assertEquals(true, false, e.getMessage());
        }
    }

    @Test
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

    @Test
    public void sqlQueryInputSourceTest()
    {
        try {
            Properties props = memsqlProps();
            MemSQLDatastore datastore = buildDataStore(props);
            SQLQueryDataset dataset = new SQLQueryDataset();
            dataset.setDatastore(datastore);
            dataset.setFetchSize(1000);
            dataset.setSqlQuery("select * from " + props.getProperty("memsql.table"));
            MemsqlComponentService service = new MemsqlComponentService();
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
        } catch(Exception e) {
            Assertions.assertEquals(true, false, e.getMessage());
        }
    }

    @Test
    public void outputInsertTest()
    {

        try {
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
            MemsqlComponentService service = new MemsqlComponentService();
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

        } catch(Exception e)
        {
            Assertions.assertEquals(true, false, e.getMessage());
        }

    }

    @Test
    public void outputBulkTest()
    {

        try {
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
            MemsqlComponentService service = new MemsqlComponentService();
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

        } catch(Exception e)
        {
            Assertions.assertEquals(true, false, e.getMessage());
        }

    }
}