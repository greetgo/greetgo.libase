package kz.greetgo.libase.utils;

import java.sql.Connection;

public interface DbWorker {
  void recreateDb(DbSide dbSide) throws Exception;

  Connection connection(DbSide dbSide) throws Exception;
}
