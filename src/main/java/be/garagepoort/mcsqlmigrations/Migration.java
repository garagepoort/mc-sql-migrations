package be.garagepoort.mcsqlmigrations;

public interface Migration {

    String getStatement();

    int getVersion();
}
