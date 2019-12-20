package com.memsql.talend.components.output.statement.operations;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Date;

public class BulkLoadWriter {
    private static BulkLoadWriter instance = null;
    private String filePath = null;
    private File dir = new File("." + File.separator + "memsql_bulk_loader");
    private BufferedWriter bulkFile;
    private static final transient Logger LOG = LoggerFactory.getLogger(BulkLoadWriter.class);

    public static BulkLoadWriter getInstance(String tableName) {
        if (instance == null)
            instance = new BulkLoadWriter(tableName);

        return instance;
    }

    private BulkLoadWriter(String tableName) {
        String path = ".";

        try {
            if (!dir.exists())
                dir.mkdir();
            path = dir.getCanonicalPath();
            filePath = path + File.separator + tableName +
                    "_" + new Date().getTime() + ".csv";
            if (filePath.contains("\\"))
                filePath = filePath.replaceAll("\\\\", "/");

            FileWriter fileWriter = new FileWriter(new File(filePath), true);
            bulkFile = new BufferedWriter(fileWriter);
        } catch(IOException e) {
           // LOG.error(e.getMessage());
        }
    }

    public void write(String data) {
        try {
            this.bulkFile.write(data);
            this.bulkFile.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public void close() {
        try {
            this.bulkFile.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public String getFilePath() {
        return filePath;
    }

    public void delete() {
        new File(filePath).delete();
    }
}
