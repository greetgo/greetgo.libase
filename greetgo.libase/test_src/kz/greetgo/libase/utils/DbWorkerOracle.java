package kz.greetgo.libase.utils;

import kz.greetgo.conf.sys_params.SysParams;
import kz.greetgo.libase.errors.IllegalOracleVersion;
import kz.greetgo.libase.errors.NoDbObject;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import static kz.greetgo.libase.utils.TestUtil.exec;

public class DbWorkerOracle extends AbstractDbWorker {

  private static String URL = "jdbc:oracle:thin:@"
    + SysParams.oracleAdminHost() + ":" + SysParams.oracleAdminPort() + ":" + SysParams.oracleAdminSid();

  private static Connection getAdminConnection() throws SQLException {
    return DriverManager.getConnection(URL, SysParams.oracleAdminUserid(), SysParams.oracleAdminPassword());
  }

  public ConnectParams connectParams(DbSide dbSide) {
    String db = System.getProperty("user.name") + "_libase_" + dbSide.name().toLowerCase();
    //noinspection UnnecessaryLocalVariable
    String username = db;
    String password = "111";
    return new StoredConnectParams(db, URL, username, password);
  }

  @Override
  public Connection connection(DbSide dbSide) throws Exception {
    Class.forName("oracle.jdbc.driver.OracleDriver");
    ConnectParams params = connectParams(dbSide);
    return DriverManager.getConnection(params.url(), params.username(), params.password());
  }

  @Override
  public void recreateDb(DbSide dbSide) throws Exception {
    Class.forName("oracle.jdbc.driver.OracleDriver");

    try (Connection con = getAdminConnection()) {

      try {
        exec(con, "alter session set \"_ORACLE_SCRIPT\"=true");
      } catch (IllegalOracleVersion ignore) {}

      ConnectParams params = connectParams(dbSide);

      try {
        exec(con, "drop user " + params.username() + " cascade");
      } catch (NoDbObject ignore) {}

      exec(con, "create user " + params.username() + " identified by " + params.password());
      exec(con, "grant all privileges to " + params.username());
    }

  }
}
