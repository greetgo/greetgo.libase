package kz.greetgo.libase.utils;

import kz.greetgo.libase.errors.NoDbObject;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class TestUtil {
  public static void exec(Connection con, String sql) {
    try (PreparedStatement ps = con.prepareStatement(sql)) {
      ps.executeUpdate();
      System.out.println("EXEC SQL: " + sql);
    } catch (SQLException e) {
      throw convertError(e);
    }
  }

  private static RuntimeException convertError(SQLException e) {
    if ("3D000".equals(e.getSQLState())) throw new NoDbObject(e);
    if ("42704".equals(e.getSQLState())) throw new NoDbObject(e);
    return new RuntimeException("SQL State = " + e.getSQLState() + " : " + e.getMessage(), e);
  }
}
