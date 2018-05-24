package kz.greetgo.libase.errors;

import java.sql.SQLException;

public class IllegalOracleVersion extends RuntimeException {
  public IllegalOracleVersion(SQLException e) {
    super("SQL State = " + e.getSQLState() + " : " + e.getMessage(), e);
  }
}
