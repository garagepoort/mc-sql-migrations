package be.garagepoort.mcsqlmigrations;

public class SqlMigrationException extends RuntimeException {
    public SqlMigrationException(String message, Throwable cause) {
        super(message, cause);
    }
}
