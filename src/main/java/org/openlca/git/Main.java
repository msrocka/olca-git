package org.openlca.git;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.eclipse.jgit.internal.storage.file.FileRepository;
import org.eclipse.jgit.lib.CommitBuilder;
import org.eclipse.jgit.lib.Constants;
import org.eclipse.jgit.lib.FileMode;
import org.eclipse.jgit.lib.PersonIdent;
import org.eclipse.jgit.lib.TreeFormatter;
import org.eclipse.jgit.revwalk.RevWalk;
import org.eclipse.jgit.treewalk.TreeWalk;

public class Main {

  public static void main(String[] args) {
    try {

      // printContent(new File(".git"));
      var author = new PersonIdent("msrocka", "test@some.mail.com");

      var gitDir = new File("./target/testrepo");
      var repo = new FileRepository(gitDir);
      if (!gitDir.exists()) {
        repo.create(true); // create a bare repo
      }

      try (var inserter = repo.newObjectInserter()) {
        var blobID = inserter.insert(Constants.OBJ_BLOB, "test".getBytes());
        var tree = new TreeFormatter();
        tree.append("test.txt", FileMode.REGULAR_FILE, blobID);
        var treeID = tree.insertTo(inserter);
        var commit = new CommitBuilder();
        commit.setAuthor(author);
        commit.setCommitter(author);
        commit.setMessage("first commit");
        commit.setEncoding(StandardCharsets.UTF_8);
        commit.setTreeId(treeID);
        var commitID = inserter.insert(commit);

        var ref = repo.findRef("HEAD");
        var update = repo.updateRef(ref.getName());
        update.setNewObjectId(commitID);
        update.update();
      }
      repo.close();

      printContent(gitDir);
      /*
       * var gitDir = new File("./target/.git"); Repository repo; if
       * (gitDir.exists()) { repo = new FileRepository(gitDir); } else { repo =
       * FileRepositoryBuilder .create(gitDir); repo.create(); }
       *
       * var id = repo.newObjectInserter() .insert(Constants.OBJ_BLOB,
       * "hello".getBytes());
       *
       * System.out.println(id.getName());
       *
       */
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private static void printContent(File gitDir) {
    try {
      var repo = new FileRepository(gitDir);
      var refs = repo.getRefDatabase();
      var objects = repo.getObjectDatabase();

      var head = refs.findRef("HEAD");
      var rev = new RevWalk(repo);
      var commit = rev.parseCommit(head.getObjectId());
      var tree = commit.getTree();

      var walk = new TreeWalk(repo);
      walk.addTree(tree);
      walk.setRecursive(false);
      while (walk.next()) {
        if (walk.isSubtree()) {
          print("dir: " + walk.getPathString());
          walk.enterSubtree();
        } else {
          print("file: " + walk.getPathString());
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private static void print(Object s) {
    System.out.println(s);
  }
}
