package com.memsql.talend.components.output.statement.operations;

import com.memsql.talend.components.output.OutputConfiguration;
import com.memsql.talend.components.output.RecordToBulkLoadConverter;
import com.memsql.talend.components.output.Reject;
import com.memsql.talend.components.service.I18nMessage;
import com.memsql.talend.components.service.MemsqlComponentService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;

import java.io.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;

public class BulkLoad extends QueryManagerImpl {
    private static final transient Logger LOG = LoggerFactory.getLogger(BulkLoad.class);
    private Map<Integer, Schema.Entry> namedParams;
    private Writer bulkFile;
    private String filePath = null;
    private File dir = new File("." + File.separator + "memsql_bulk_loader");

    public BulkLoad(final OutputConfiguration configuration, final I18nMessage i18n) {
        super(i18n, configuration);

        String path = ".";
        FileWriter fileWriter = null;

        try {
            if (!dir.exists())
                dir.mkdir();
            path = dir.getCanonicalPath();
            filePath = path + File.separator + getConfiguration().getDataset().getTableName() +
                    "_" + new Date().getTime() + ".csv";
            if (File.separator.equals("\\"))
                filePath.replaceAll("\\\\", "/");
            fileWriter = new FileWriter(new File(filePath));
        } catch(IOException e) {
            LOG.error(e.getMessage());
        }
        bulkFile = new BufferedWriter(fileWriter);
    }

    @Override
    protected String buildQuery(List<Record> records) {
        final List<Schema.Entry> entries = records.stream().flatMap(r -> r.getSchema().getEntries().stream()).distinct()
                .collect(toList());
        namedParams = new HashMap<>();
        final AtomicInteger index = new AtomicInteger(0);
        entries.forEach(name -> namedParams.put(index.incrementAndGet(), name));

        return null;
    }

    @Override
    protected Map<Integer, Schema.Entry> getQueryParams() {
        return namedParams;
    }

    @Override
    protected boolean validateQueryParam(final Record record) {
        return namedParams.values().stream().filter(e -> !e.isNullable()).map(e -> valueOf(record, e))
                .allMatch(Optional::isPresent);
    }

    @Override
    public List<Reject> execute(final List<Record> records, final MemsqlComponentService.DataSource dataSource) throws SQLException {
        if (records.isEmpty()) {
            return emptyList();
        }
        try (final Connection connection = dataSource.getConnection()) {
            buildQuery(records);
            return processRecords(records, connection);
        }
    }

    private List<Reject> processRecords(final List<Record> records, final Connection connection)
            throws SQLException {
        List<Reject> rejects = new ArrayList<Reject>();

        StringBuilder buffer = new StringBuilder();
        List<String> lines = new ArrayList<String>();


        for (final Record record : records) {
            buffer.setLength(0);
            if (!validateQueryParam(record)) {
                rejects.add(new Reject("missing required query param in this record", record));
                continue;
            }
            for (final Map.Entry<Integer, Schema.Entry> entry : getQueryParams().entrySet()) {
                RecordToBulkLoadConverter.valueOf(entry.getValue().getType().name()).setValue(lines, entry.getKey(),
                        entry.getValue(), record);
            }
            buffer.append(lines.stream().collect(Collectors.joining(","))+"\n");
            try {
                bulkFile.write(buffer.toString());
                bulkFile.flush();
            } catch(IOException e)
            {
                LOG.error(e.getMessage());
            }
            lines.clear();
        }


        return rejects;
    }

    @Override
    public void load(final Connection connection) throws SQLException {
        long start = new Date().getTime();
        try {
            bulkFile.flush();
            bulkFile.close();
        } catch (IOException e) {
            LOG.error(e.getMessage());
        }
        System.out.println(filePath);
        int rows = connection.createStatement().executeUpdate("LOAD DATA LOCAL INFILE '"+filePath+"' INTO TABLE " + getConfiguration().getDataset().getTableName() + " FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' LINES TERMINATED BY '\\n'");
        LOG.debug("Rows Processed " + rows + "  for file " + filePath);
        connection.commit();


        System.out.println("Load Time " + (new Date().getTime() - start));
    }
}