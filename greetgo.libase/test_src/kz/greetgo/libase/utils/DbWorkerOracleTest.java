package kz.greetgo.libase.utils;

public class DbWorkerOracleTest extends DbWorkerPostgresTest {
  protected DbWorker dbWorker() {
    return new DbWorkerOracle();
  }
}
