package be.garagepoort.mcsqlmigrations;

import java.sql.Connection;
import java.util.List;

public interface Migration {

    default String getStatement(Connection connection) { return null; };

    default List<String> getStatements(Connection connection) { return null; };

    int getVersion();
}
