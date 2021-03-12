package org.openlca.git;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.List;

import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;

import org.eclipse.jgit.internal.storage.file.FileRepository;
import org.eclipse.jgit.lib.FileMode;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.ObjectInserter;
import org.eclipse.jgit.lib.PersonIdent;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.lib.TreeFormatter;
import org.openlca.core.database.Derby;
import org.openlca.core.database.IDatabase;
import org.openlca.core.model.Actor;
import org.openlca.core.model.ModelType;
import org.openlca.core.model.descriptors.Descriptor;
import org.openlca.git.DbTree.Node;
import org.openlca.proto.output.ActorWriter;
import org.openlca.proto.output.WriterConfig;

public class RepoWriter {

  private final IDatabase db;
  private final Repository repo;
  private final PersonIdent committer;

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

    try (var inserter = repo.newObjectInserter()) {
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

    }
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
      var data = serialize(d);
      if (data == null)
        continue;
    }
  }

  private byte[] serialize(Descriptor d) {
    var model = db.get(d.type.getModelClass(), d.id);
    if (model == null)
      return null;

    Message message = null;
    if (model instanceof Actor) {
      var w = new ActorWriter(WriterConfig.of(db));
      message = w.write((Actor) model);
    }

    if (message == null)
      return null;
    try {
      var json = JsonFormat.printer().print(message);
      return json.getBytes(StandardCharsets.UTF_8);
    } catch (Exception e) {
      return null;
    }
  }

  public static void main(String[] args) {

    var committer = new PersonIdent("msrocka", "test@some.mail.com");
    var repoDir = new File("target/testrepo/.git");

    try (var db = Derby.fromDataDir("ei22");
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
