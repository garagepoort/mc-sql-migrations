package be.garagepoort.mcsqlmigrations;

import javax.sql.DataSource;
import java.sql.Connection;
import java.util.List;

public interface SqlConnectionProvider {

    Connection getConnection();

    DataSource getDatasource();

    List<String> getMigrationPackages();

    DatabaseType getDatabaseType();
}
