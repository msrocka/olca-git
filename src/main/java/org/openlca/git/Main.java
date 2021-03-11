package org.openlca.git;

public class Main {

  public static void main(String[] args) {
    try {

      var gitDir = new File("./target/.git");
      Repository repo;
      if (gitDir.exists()) {
        repo = new FileRepository(gitDir);
      } else {
        repo = FileRepositoryBuilder
            .create(gitDir);
        repo.create();
      }

      var id = repo.newObjectInserter()
          .insert(Constants.OBJ_BLOB, "hello".getBytes());

      System.out.println(id.getName());

    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
