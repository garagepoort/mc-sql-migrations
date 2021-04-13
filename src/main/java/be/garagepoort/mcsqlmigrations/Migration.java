package be.garagepoort.mcsqlmigrations;

import java.util.List;

public interface Migration {

    default String getStatement() { return null; };

    default List<String> getStatements() { return null; };

    int getVersion();
}
