package kz.greetgo.libase.utils;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class DbWorkerPostgresTest {

  protected DbWorker dbWorker() {
    return new DbWorkerPostgres();
  }

  @DataProvider
  public Object[][] allDbSides() {
    Object[][] ret = new Object[DbSide.values().length][];
    for (int i = 0; i < DbSide.values().length; i++) {
      ret[i] = new Object[]{DbSide.values()[i]};
    }
    return ret;
  }

  @Test(dataProvider = "allDbSides")
  public void recreateDb(DbSide dbSide) throws Exception {
    dbWorker().recreateDb(dbSide);
  }
}
