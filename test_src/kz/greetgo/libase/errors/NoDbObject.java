package kz.greetgo.libase.errors;

import java.sql.SQLException;

public class NoDbObject extends RuntimeException {
  public NoDbObject(SQLException e) {
    super("SQL State = " + e.getSQLState() + " : " + e.getMessage(), e);
  }
}
