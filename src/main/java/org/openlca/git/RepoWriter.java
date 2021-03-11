package org.openlca.git;

import org.eclipse.jgit.lib.Repository;
import org.openlca.core.database.CategoryDao;
import org.openlca.core.database.IDatabase;

public class RepoWriter {

  private final Repository repo;
  private final IDatabase db;

  public RepoWriter(Repository repo, IDatabase db) {
    this.repo = repo;
    this.db = db;
  }

  public void sync() {
    var tree = Tree.build(db);
    var dao = new CategoryDao(db);
  }

}
