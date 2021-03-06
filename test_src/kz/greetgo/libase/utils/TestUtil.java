package kz.greetgo.libase.utils;

import kz.greetgo.libase.errors.IllegalOracleVersion;
import kz.greetgo.libase.errors.NoDbObject;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class TestUtil {
  public static void exec(Connection con, String sql) {

    try (Statement statement = con.createStatement()) {
      statement.execute(sql);
      System.out.println("EXEC SQL: " + sql);
    } catch (SQLException e) {
      throw convertError(e);
    }
  }

  private static RuntimeException convertError(SQLException e) {
    if ("3D000".equals(e.getSQLState())) throw new NoDbObject(e);
    if ("42704".equals(e.getSQLState())) throw new NoDbObject(e);
    if (e.getMessage().startsWith("ORA-01918:")) throw new NoDbObject(e);
    if (e.getMessage().startsWith("ORA-02248:")) throw new IllegalOracleVersion(e);
    return new RuntimeException("SQL State = " + e.getSQLState() + " : " + e.getMessage(), e);
  }
}
