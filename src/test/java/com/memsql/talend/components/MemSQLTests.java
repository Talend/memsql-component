package com.memsql.talend.components;

import java.lang.reflect.Field;
import java.util.Properties;

import javax.annotation.PostConstruct;

import com.memsql.talend.components.dataset.SQLQueryDataset;
import com.memsql.talend.components.dataset.TableNameDataset;
import com.memsql.talend.components.datastore.MemSQLDatastore;
import com.memsql.talend.components.service.I18nMessage;
import com.memsql.talend.components.service.MemsqlComponentService;
import com.memsql.talend.components.source.SQLQueryInputMapperConfiguration;
import com.memsql.talend.components.source.SQLQueryInputSource;
import com.memsql.talend.components.source.TableNameInputMapperConfiguration;
import com.memsql.talend.components.source.TableNameInputSource;

import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.shadow.com.univocity.parsers.common.record.RecordFactory;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.runtime.record.RecordBuilderFactoryImpl;

public class MemSQLTests extends MemSQLBaseTest {


    @Test
    public void initialize() {
        Properties props = memsqlProps();
        // Determine if properties file read. Test docker.name
        Assertions.assertEquals("memsql/cluster-in-a-box", props.getProperty("docker.name"));

        // Test if memsql cluster in a box docker image read
        boolean result = memsqlDockerConfig(props);
        Assertions.assertEquals(true, result);

        // Finally test if able to initialize MemSQL Docker instance for Component Tests
        result = initializeMemSQL(props);
        Assertions.assertEquals(true, result);


        
        /*
        try {
            shutdownDockerConfig();
            Assertions.assertEquals(true, true);
        } catch(Exception e)
        {
            System.err.println(e.getMessage());
            Assertions.assertEquals(true, false);
        }
        */
    }

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
            e.printStackTrace();
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
            e.printStackTrace();
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
            e.printStackTrace();
        }
    }
}