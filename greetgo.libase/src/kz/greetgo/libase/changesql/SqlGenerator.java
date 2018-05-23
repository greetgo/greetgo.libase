package kz.greetgo.libase.changesql;

import java.util.List;

import kz.greetgo.libase.changes.Change;

public interface SqlGenerator {
  void generate(List<String> sqlResult, List<Change> changes);
}
