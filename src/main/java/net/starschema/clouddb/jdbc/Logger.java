package net.starschema.clouddb.jdbc;

import java.util.logging.Level;

/**
 * Wrapper class around {@link java.util.logging.Logger} to implement a log4j style interface.
 */
public class Logger {

    private java.util.logging.Logger logger;

    public static Logger getLogger(Object tag) {
        return new Logger(objToMessage(tag));
    }

    private Logger(String tag) {
        logger = java.util.logging.Logger.getLogger(tag);
        // Print all logs.
        logger.setLevel(Level.ALL);
    }

    public void debug(Object message) {
        logger.log(Level.FINEST, objToMessage(message));
    }

    public void debug(Object message, Throwable e) {
        logger.log(Level.FINE, objToMessage(message), e);
    }

    public void info(Object message) {
       logger.log(Level.INFO, objToMessage(message));
    }

    public void info(Object message, Throwable e) {
        logger.log(Level.INFO, objToMessage(message), e);
    }

    public void error(Object message) {
        logger.log(Level.SEVERE, objToMessage(message));
    }

    public void error(Object message, Throwable e) {
        logger.log(Level.SEVERE, objToMessage(message), e);
    }

    public void warn(Object message) {
        logger.log(Level.WARNING, objToMessage(message));
    }

    public void warn(Object message, Throwable e) {
        logger.log(Level.WARNING, objToMessage(message), e);
    }

    public void fatal(Object message) {
        error(message);
    }

    public void fatal(Object message, Throwable e) {
        error(message, e);
    }

    private static String objToMessage(Object message) {
        return message == null ? "null" : message.toString();
    }
}
