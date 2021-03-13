package org.openlca.git;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;

import org.eclipse.jgit.internal.storage.file.FileRepository;
import org.eclipse.jgit.lib.CommitBuilder;
import org.eclipse.jgit.lib.Constants;
import org.eclipse.jgit.lib.FileMode;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.PersonIdent;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.lib.TreeFormatter;
import org.openlca.core.database.Derby;
import org.openlca.core.database.IDatabase;
import org.openlca.core.model.ModelType;
import org.openlca.core.model.Version;
import org.openlca.git.DbTree.Node;
import org.openlca.util.Pair;
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

    // build the tree
    var tree = new TreeFormatter();
    for (var type : ModelType.values()) {
      var node = dbTree.getRoot(type);
      if (node == null)
        continue;
      var id = syncTree(node);
      if (id != null) {
        tree.append(type.name(), FileMode.TREE, id);
      }
    }
    commit(tree);

    time = System.currentTimeMillis() - start;
    System.out.printf("synced the tree in %.3f sec%n", time / 1000d);
  }

  private void commit(TreeFormatter tree) {
    try (var inserter = repo.newObjectInserter()) {

      // create the commit
      var treeID = tree.insertTo(inserter);
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
  }

  private ObjectId syncTree(Node node) {
    if (node.content.isEmpty() && node.childs.isEmpty())
      return null;

    // first sync the child trees
    var tree = new TreeFormatter();
    for (var child : node.childs) {
      var childID = syncTree(child);
      if (childID != null) {
        tree.append(child.name, FileMode.TREE, childID);
      }
    }
    if (node.content.isEmpty())
      return insertTree(tree);

    // try to convert and write the data sets with multiple threads
    var threads = Executors.newFixedThreadPool(8);
    var dataQueue = new ArrayBlockingQueue<byte[]>(50);
    var blobQueue = new ArrayBlockingQueue<Pair<String, ObjectId>>(50);
    Pair<String, ObjectId> empty = Pair.of(null, null);

    for (var d : node.content) {
      if (d.type == null || d.type.getModelClass() == null)
        continue;

      // load and convert the data set into a byte array and
      // add it to the queue.
      threads.submit(() -> {
        try {
          var entity = db.get(d.type.getModelClass(), d.id);
          var data = ProtoWriter.toJson(entity, db);
          dataQueue.add(data == null ? new byte[0] : data);
        } catch (Exception e) {
          var log = LoggerFactory.getLogger(getClass());
          log.error("failed to convert to proto: " + d, e);
          dataQueue.add(new byte[0]);
        }
      });

      // get a data package and write it to the blob store
      threads.submit(() -> {
        var data = dataQueue.poll();
        if (data == null || data.length == 0) {
          blobQueue.add(empty);
        }
        try (var inserter = repo.newObjectInserter()) {
          var blobID = inserter.insert(Constants.OBJ_BLOB, data);
          var name = d.refId + "_" + Version.asString(d.version) + ".json";
          blobQueue.add(Pair.of(name, blobID));
        } catch (Exception e) {
          var log = LoggerFactory.getLogger(getClass());
          log.error("failed to insert blob for " + d, e);
          blobQueue.add(empty);
        }
      });

      // add blobs to the tree in the main thread
      var next = blobQueue.poll();
      if (next != null && next != empty) {
        tree.append(next.first, FileMode.REGULAR_FILE, next.second);
      }
    }

    // we do not need to wait for the threads to finish here as
    // everything should be synchronized via the queues
    threads.shutdown();
    return insertTree(tree);
  }

  private ObjectId insertTree(TreeFormatter tree) {
    try (var inserter = repo.newObjectInserter()) {
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
