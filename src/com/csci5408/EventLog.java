package com.csci5408;

import java.io.File;
import java.io.IOException;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class EventLog {
    public Logger logger;
    FileHandler fileHandler;
    public EventLog(String file_name) throws SecurityException, IOException {
        File f = new File(file_name);
        if(!f.exists()){
            f.createNewFile();
        }

        fileHandler = new FileHandler(file_name, true);
        logger = Logger.getLogger("test1");
        logger.addHandler(fileHandler);
        SimpleFormatter formatter = new SimpleFormatter();
        fileHandler.setFormatter(formatter);
    }
}
