package be.garagepoort.mcsqlmigrations;

public class MigrationsException extends RuntimeException {

    public MigrationsException(Throwable e) {
        super("Mc-sql-migrations exception:",e);
    }
}
