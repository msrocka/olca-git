package org.openlca.git;

import java.io.File;
import java.nio.charset.StandardCharsets;

import org.eclipse.jgit.internal.storage.file.FileRepository;
import org.eclipse.jgit.lib.CommitBuilder;
import org.eclipse.jgit.lib.Constants;
import org.eclipse.jgit.lib.FileMode;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.ObjectInserter;
import org.eclipse.jgit.lib.PersonIdent;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.lib.TreeFormatter;
import org.openlca.core.database.Derby;
import org.openlca.core.database.IDatabase;
import org.openlca.core.model.ModelType;
import org.openlca.core.model.Version;
import org.openlca.git.DbTree.Node;
import org.slf4j.LoggerFactory;

public class RepoWriter {

  private final IDatabase db;
  private final Repository repo;
  private final PersonIdent committer;

  // private final byte[] FINISH_MARKER = new byte[0];

  public RepoWriter(
    IDatabase db,
    Repository repo,
    PersonIdent committer) {
    this.repo = repo;
    this.db = db;
    this.committer = committer;
  }

  public void sync() {
    var start = System.currentTimeMillis();
    var dbTree = DbTree.build(db);
    var time = System.currentTimeMillis() - start;
    System.out.printf("loaded DB tree in %.3f sec%n", time / 1000d);

    start = System.currentTimeMillis();
    try (var inserter = repo.newObjectInserter()) {

      // build the tree
      var root = new TreeFormatter();
      for (var type : ModelType.values()) {
        var node = dbTree.getRoot(type);
        if (node == null)
          continue;
        var id = syncTree(inserter, node);
        if (id != null) {
          root.append(type.name(), FileMode.TREE, id);
        }
      }

      // create the commit
      var treeID = root.insertTo(inserter);
      var commit = new CommitBuilder();
      commit.setAuthor(committer);
      commit.setCommitter(committer);
      commit.setMessage("first commit");
      commit.setEncoding(StandardCharsets.UTF_8);
      commit.setTreeId(treeID);
      var commitID = inserter.insert(commit);

      // update the head
      var ref = repo.findRef("HEAD");
      var update = repo.updateRef(ref.getName());
      update.setNewObjectId(commitID);
      update.update();

    } catch (Exception e) {
      var log = LoggerFactory.getLogger(getClass());
      log.error("failed to sync tree", e);
    }

    time = System.currentTimeMillis() - start;
    System.out.printf("synced the tree in %.3f sec%n", time / 1000d);
  }

  private ObjectId syncTree(ObjectInserter inserter, Node node) {
    if (node.content.isEmpty() && node.childs.isEmpty())
      return null;
    var tree = new TreeFormatter();
    for (var child : node.childs) {
      var childID = syncTree(inserter, child);
      if (childID != null) {
        tree.append(child.name, FileMode.TREE, childID);
      }
    }

    for (var d : node.content) {
      if (d.type == null || d.type.getModelClass() == null)
        continue;
      var entity = db.get(d.type.getModelClass(), d.id);
      if (entity == null)
        continue ;
      var data = ProtoWriter.toJson(entity, db);
      if (data == null)
        continue;

      try {
        var blobID = inserter.insert(Constants.OBJ_BLOB, data);
        var name = d.refId + "_" + Version.asString(d.version);
        tree.append(name, FileMode.REGULAR_FILE, blobID);
      } catch (Exception e) {
        var log = LoggerFactory.getLogger(getClass());
        log.error("failed to insert blob for " + d, e);
      }
    }

    try {
      return tree.insertTo(inserter);
    } catch (Exception e) {
      var log = LoggerFactory.getLogger(getClass());
      log.error("failed to insert tree", e);
      return null;
    }
  }

  public static void main(String[] args) {

    var committer = new PersonIdent("msrocka", "test@some.mail.com");
    var repoDir = new File("target/testrepo/.git");

    try (var db = Derby.fromDataDir("refdb");
         var repo = new FileRepository(repoDir)) {
      if (!repoDir.exists()) {
        repo.create(true); // bare repo
      }
      new RepoWriter(db, repo, committer).sync();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

}
