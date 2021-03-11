package org.openlca.git;

import org.eclipse.jgit.lib.Repository;
import org.openlca.core.database.CategoryDao;
import org.openlca.core.database.Derby;
import org.openlca.core.database.IDatabase;

public class RepoWriter {

  private final Repository repo;
  private final IDatabase db;

  public RepoWriter(Repository repo, IDatabase db) {
    this.repo = repo;
    this.db = db;
  }

  public void sync() {
    var tree = DbTree.build(db);
    var dao = new CategoryDao(db);
  }

  public static void main(String[] args) {
    try (var db = Derby.fromDataDir("ei37-apos")) {
      var start = System.currentTimeMillis();
      DbTree.build(db);
      var time = System.currentTimeMillis() - start;
      System.out.printf("loading took: %.3f sec%n", time / 1000d);
    }
  }

}
