package be.garagepoort.mcsqlmigrations.helpers;

public class DatabaseException extends RuntimeException {
    public DatabaseException(Throwable cause) {
        super(cause);
    }

    public DatabaseException(String error, Throwable e) {
        super(error, e);
    }
}
