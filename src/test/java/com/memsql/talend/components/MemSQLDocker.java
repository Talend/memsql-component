package com.memsql.talend.components;

import java.util.Properties;

public class MemSQLDocker extends MemSQLBaseTest {
    public static void main(String[] args)
    {
        MemSQLDocker instance = new MemSQLDocker();
        if (args.length != 1) instance.exit(1,"args length not correct. Args Length " + args.length);
        if (!args[0].equalsIgnoreCase("start") || !args[0].equalsIgnoreCase("stop")) instance.exit (1, "Not proper command of start|stop. Value: " + args[0]);
        if (args[0].equalsIgnoreCase("start"))
            instance.start();
        else
            instance.stop();
    }
    
    private void start()
    {
        Properties props = memsqlProps();
        boolean result = memsqlDockerConfig(props);
        if (!result) exit(1, "Error Setup with MemSQL Docker container");
        result = initializeMemSQL(props);
        if (!result) exit(1, "Error intializing MemSQL Database");

    }

    private void stop()
    {
        try {
            shutdownDockerConfig();
        } catch(Exception e)
        {
            exit(1, e.getMessage());
        }
    }

    private void exit(int code, String message) {
        System.out.println(message);
        System.exit(code);
    }
}