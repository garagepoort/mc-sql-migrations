package be.garagepoort.mcsqlmigrations;

import javax.sql.DataSource;
import java.sql.Connection;

public interface SqlConnectionProvider {

    Connection getConnection();

    DataSource getDatasource();

    DatabaseType getDatabaseType();
}
