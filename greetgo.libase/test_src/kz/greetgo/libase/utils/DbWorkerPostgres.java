package kz.greetgo.libase.utils;

import kz.greetgo.conf.sys_params.SysParams;
import kz.greetgo.libase.errors.NoDbObject;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import static kz.greetgo.libase.utils.TestUtil.exec;

public class DbWorkerPostgres extends AbstractDbWorker {

  private static Connection getAdminConnection() throws SQLException {
    return DriverManager.getConnection(SysParams.pgAdminUrl(), SysParams.pgAdminUserid(), SysParams.pgAdminPassword());
  }

  private static String changeDb(String url, String db) {
    int index = url.lastIndexOf("/");
    if (index < 0) throw new IllegalArgumentException("Left url = " + url);
    return url.substring(0, index + 1) + db;
  }

  public ConnectParams connectParams(DbSide dbSide) {
    String db = System.getProperty("user.name") + "_libase_" + dbSide.name().toLowerCase();
    //noinspection UnnecessaryLocalVariable
    String username = db;
    String password = "111";

    String url = changeDb(SysParams.pgAdminUrl(), db);

    return new StoredConnectParams(db, url, username, password);
  }

  @Override
  public Connection connection(DbSide dbSide) throws Exception {
    Class.forName("org.postgresql.Driver");
    ConnectParams params = connectParams(dbSide);
    return DriverManager.getConnection(params.url(), params.username(), params.password());
  }

  @Override
  public void recreateDb(DbSide dbSide) throws Exception {
    Class.forName("org.postgresql.Driver");

    try (Connection con = getAdminConnection()) {

      ConnectParams params = connectParams(dbSide);

      try {
        exec(con, "drop database " + params.db());
      } catch (NoDbObject ignore) {}

      try {
        exec(con, "drop user " + params.username());
      } catch (NoDbObject ignore) {}

      exec(con, "create user " + params.username() + " with password '" + params.password() + "'");
      exec(con, "create database " + params.db() + " with owner = '" + params.username() + "'");

      try (Connection connection = connection(dbSide)) {
        exec(connection, "create schema moon");
        exec(connection, "create schema boom");
      }

    }

  }

}
