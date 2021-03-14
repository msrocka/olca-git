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
import org.eclipse.persistence.jpa.jpql.EclipseLinkVersion;
import org.eclipse.persistence.jpa.jpql.JPAVersion;
import org.openlca.core.database.Derby;

public class Main {

  public static void main(String[] args) {
    try {

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

      // printContent(repoDir);
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
