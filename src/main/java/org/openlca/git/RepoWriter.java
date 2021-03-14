package org.openlca.git;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.eclipse.jgit.internal.storage.file.FileRepository;
import org.eclipse.jgit.lib.CommitBuilder;
import org.eclipse.jgit.lib.Constants;
import org.eclipse.jgit.lib.FileMode;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.PersonIdent;
import org.eclipse.jgit.lib.TreeFormatter;
import org.openlca.core.database.IDatabase;
import org.openlca.core.model.ModelType;
import org.openlca.core.model.Version;
import org.openlca.git.DbTree.Node;
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
    // and synchronize them with a blocking queue
    var config = Converter.newJsonConfig(db);

    // start a single writer thread that waits for the converted data sets
    var writer = threads.submit(() -> {
      var inserter = repo.getObjectDatabase().newPackInserter();
      inserter.checkExisting(false);
      // we must get a result (which can be empty) for each data set
      for (int i = 0; i < node.content.size(); i++) {
        try {
          var next = config.queue.take();
          if (next == config.EMPTY)
            continue;
          var d = next.first;
          var blobID = inserter.insert(Constants.OBJ_BLOB, next.second);
          var name = d.refId + "_" + Version.asString(d.version) + ".json";
          tree.append(name, FileMode.REGULAR_FILE, blobID);
        } catch (Exception e) {
          log.error("failed to write data set", e);
        }
      }

      // close the inserter
      try {
        inserter.flush();
        inserter.close();
      } catch (IOException e) {
        log.error("failed to flush objects", e);
      }

    });

    // start the converters in blocks of threads
    var workerCount = 8;
    var futures = new ArrayList<Future<?>>(workerCount);
    int total = node.content.size();
    int offset = 0;
    while (offset < total) {

      // start a new block of converters
      for (int i = 0; i < workerCount; i++) {
        if (offset >= total)
          break;
        var descriptor = node.content.get(offset);
        var future = threads.submit(
          new Converter(config, descriptor));
        futures.add(future);
        offset++;
      }
      if (futures.isEmpty())
        break;

      // wait for the converters to finish
      try {
        for(var future : futures) {
          future.get();
        }
      } catch (Exception e) {
        log.error("interrupted conversion", e);
      } finally {
        futures.clear();
      }
    }

    // wait for the writer to finish
    try {
      writer.get();
    } catch (Exception e) {
      log.error("failed to finish the writer thread", e);
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

}
