package net.starschema.clouddb.jdbc;

public class Logger {

    static public
    Logger getLogger(Object name) {
        return new Logger();
    }

    private Logger() {
    }

    public void debug(Object message) {
        System.err.println(message);
    }
    public void debug(Object message, Throwable e) {
        System.err.println(message + " " + e.toString());
    }
    public void info(Object message) {
        System.err.println(message);
    }
    public void info(Object message, Throwable e) {
        System.err.println(message + " " + e.toString());
    }
    public void error(Object message) {
        System.err.println(message);
    }
    public void error(Object message, Throwable e) {
        System.err.println(message + " " + e.toString());
    }
    public void warn(Object message) {
        System.err.println(message);
    }
    public void warn(Object message, Throwable e) {
        System.err.println(message + " " + e.toString());
    }
    public void fatal(Object message) {
        System.err.println(message);
    }
    public void fatal(Object message, Throwable e) {
        System.err.println(message + " " + e.toString());
    }
}
