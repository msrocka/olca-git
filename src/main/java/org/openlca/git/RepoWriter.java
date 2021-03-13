package org.openlca.git;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

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
import org.openlca.core.model.descriptors.Descriptor;
import org.openlca.git.DbTree.Node;
import org.openlca.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RepoWriter {

  private final Logger log = LoggerFactory.getLogger(getClass());
  private final IDatabase db;
  private final FileRepository repo;
  private final PersonIdent committer;

  public RepoWriter(
    IDatabase db,
    FileRepository repo,
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

    var threads = Executors.newCachedThreadPool();
    // build the tree
    var tree = new TreeFormatter();
    for (var type : ModelType.values()) {
      var node = dbTree.getRoot(type);
      if (node == null)
        continue;
      var id = syncTree(node, threads);
      if (id != null) {
        tree.append(type.name(), FileMode.TREE, id);
      }
    }
    threads.shutdown();
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
      log.error("failed to sync tree", e);
    }
  }

  private ObjectId syncTree(Node node, ExecutorService threads) {
    if (node.content.isEmpty() && node.childs.isEmpty())
      return null;

    // first sync the child trees
    var tree = new TreeFormatter();
    for (var child : node.childs) {
      var childID = syncTree(child, threads);
      if (childID != null) {
        tree.append(child.name, FileMode.TREE, childID);
      }
    }
    if (node.content.isEmpty())
      return insertTree(tree);

    // try to convert and write the data sets with multiple threads
    var dataQueue = new ArrayBlockingQueue<Pair<Descriptor, byte[]>>(50);
    var blobQueue = new ArrayBlockingQueue<Pair<Descriptor, ObjectId>>(50);
    Pair<Descriptor, byte[]> dataEnd = Pair.of(null, null);
    Pair<Descriptor, ObjectId> blobEnd = Pair.of(null, null);

    // start a thread that loads the data sets and
    // converts them to byte arrays
    threads.submit(() -> {

      Consumer<Pair<Descriptor, byte[]>> onNext = pair -> {
        try {
          while (!dataQueue.offer(pair, 5, TimeUnit.SECONDS)) {
            log.warn("data queue is blocked; waiting");
          }
        } catch (InterruptedException e) {
          log.error("interrupted data adding", e);
        }
      };

      for (var d : node.content) {
        try {
          var entity = db.get(d.type.getModelClass(), d.id);
          var data = ProtoWriter.toJson(entity, db);
          if (data == null) {
            break;
          }
          onNext.accept(Pair.of(d, data));
        } catch (Exception e) {
          log.error("failed to convert to proto: " + d, e);
          break;
        }
      }
      onNext.accept(dataEnd);

    });

    // start a thread that takes the byte arrays and writes
    // them to the blob store
    threads.submit(() -> {

      Consumer<Pair<Descriptor, ObjectId>> onNext = pair -> {
        try {
          while (!blobQueue.offer(pair, 60, TimeUnit.SECONDS)) {
            log.warn("blob queue is blocked; waiting");
          }
        } catch (InterruptedException e) {
          log.error("interrupted data adding", e);
        }
      };

      var inserter = repo.getObjectDatabase().newPackInserter();
      inserter.checkExisting(false);

      while (true) {
        try {
          var next = dataQueue.take();
          if (next == dataEnd)
            break;

          var blobID = inserter.insert(Constants.OBJ_BLOB, next.second);
          onNext.accept(Pair.of(next.first, blobID));
        } catch (Exception e) {
          log.error("failed to insert blob", e);
          break;
        }
      }

      try {
        inserter.flush();
        inserter.close();
      } catch (IOException e) {
        log.error("failed to flush objects", e);
      }

      onNext.accept(blobEnd);
    });

    // add the blob IDs to the tree
    while (true) {
      try {
        var next = blobQueue.take();
        if (next == blobEnd)
          break;
        var d = next.first;
        var name = d.refId + "_" + Version.asString(d.version) + ".json";
        tree.append(name, FileMode.REGULAR_FILE, next.second);
      } catch (Exception e) {
        log.error("interruption in writer threads", e);
        break;
      }
    }

    return insertTree(tree);
  }

  private ObjectId insertTree(TreeFormatter tree) {
    try (var inserter = repo.newObjectInserter()) {
      return tree.insertTo(inserter);
    } catch (Exception e) {
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
