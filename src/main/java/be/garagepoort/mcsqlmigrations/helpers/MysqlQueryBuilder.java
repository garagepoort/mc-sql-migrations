package be.garagepoort.mcsqlmigrations.helpers;

import be.garagepoort.mcsqlmigrations.SqlConnectionProvider;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class MysqlQueryBuilder extends QueryBuilder {

    public MysqlQueryBuilder(SqlConnectionProvider connectionProvider) {
        super(connectionProvider);
    }

    @Override
    public Integer getGeneratedId(Connection connection, PreparedStatement insert) throws SQLException {
        ResultSet generatedKeys;
        generatedKeys = insert.getGeneratedKeys();
        int generatedKey = -1;
        if (generatedKeys.next()) {
            generatedKey = generatedKeys.getInt(1);
        }
        return generatedKey;
    }
}
