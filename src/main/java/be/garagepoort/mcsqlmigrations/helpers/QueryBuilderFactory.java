package be.garagepoort.mcsqlmigrations.helpers;

import be.garagepoort.mcsqlmigrations.DatabaseType;
import be.garagepoort.mcsqlmigrations.SqlConnectionProvider;

import java.sql.Connection;

public class QueryBuilderFactory {

    private final DatabaseType databaseType;
    private final SqlConnectionProvider sqlConnectionProvider;

    public QueryBuilderFactory(DatabaseType databaseType, SqlConnectionProvider sqlConnectionProvider) {
        this.databaseType = databaseType;
        this.sqlConnectionProvider = sqlConnectionProvider;
    }

    public QueryBuilder create() {
        if (databaseType == DatabaseType.MYSQL) {
            return new MysqlQueryBuilder(sqlConnectionProvider);
        }
        return new SqliteQueryBuilder(sqlConnectionProvider);
    }

    public QueryBuilder create(Connection connection) {
        if (databaseType == DatabaseType.MYSQL) {
            return new MysqlQueryBuilder(connection);
        }
        return new SqliteQueryBuilder(connection);
    }
}
