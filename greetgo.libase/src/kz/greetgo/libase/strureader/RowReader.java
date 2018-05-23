package kz.greetgo.libase.strureader;

import java.util.List;
import java.util.Map;

public interface RowReader {
  List<ColumnRow> readAllTableColumns() throws Exception;
  
  Map<String, PrimaryKeyRow> readAllTablePrimaryKeys() throws Exception;
  
  Map<String, ForeignKeyRow> readAllForeignKeys() throws Exception;
  
  Map<String, SequenceRow> readAllSequences() throws Exception;
  
  Map<String, ViewRow> readAllViews() throws Exception;
  
  List<StoreFuncRow> readAllFuncs() throws Exception;
  
  Map<String, TriggerRow> readAllTriggers() throws Exception;
  
  Map<String, String> readTableComments() throws Exception;
  
  Map<String, String> readColumnComments() throws Exception;
}
