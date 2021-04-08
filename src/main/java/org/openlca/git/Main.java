package org.openlca.git;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.eclipse.jgit.internal.storage.file.FileRepository;
import org.eclipse.jgit.lib.PersonIdent;
import org.openlca.core.database.Derby;
import org.openlca.core.database.IDatabase;
import org.openlca.core.model.ModelType;
import org.openlca.git.descriptor.Tree;
import org.openlca.git.repo.RepoWriter;
import org.openlca.util.Dirs;

public class Main {

	private static final String db = "ref_data";
	private static final PersonIdent committer = new PersonIdent("greve", "greve@greendelta.com");
	private static final File repoDir = new File("C:/Users/Sebastian/test/olca-git/" + db);

	public static void main(String[] args) throws IOException {
		if (repoDir.exists()) {
			Dirs.delete(repoDir);
		}
		try (var database = Derby.fromDataDir(db);
				var repo = new FileRepository(repoDir)) {
			repo.create(true);
			var config = Config.newProtoConfig(database, repo, committer);
			var writer = new DbWriter(config);
			writer.refData(true);
			writer.timer.print("tree");
			writer.timer.print("writer");
		}
	}

	private static class DbWriter {

		private static final ModelType[] REF_DATA_TYPES = { ModelType.UNIT_GROUP, ModelType.FLOW_PROPERTY,
				ModelType.CURRENCY, ModelType.LOCATION, ModelType.DQ_SYSTEM, ModelType.FLOW };
		private final IDatabase database;
		private final RepoWriter writer;
		private final Timer timer;

		private DbWriter(Config config) {
			this.database = config.database;
			this.writer = new RepoWriter(config);
			this.timer = new Timer();
		}

		private void refData(boolean singleCommit) {
			if (singleCommit) {
				refDataSingleCommit();
			} else {
				refDataSeparateCommits();
			}
		}

		private void refDataSingleCommit() {
			var dTree = new Tree(database);
			timer.time("tree", () -> {
				for (ModelType type : REF_DATA_TYPES) {
					dTree.addBranch(type);
				}
			});
			timer.time("writer", () -> writer.commit(dTree, "Added data"));
		}

		private void refDataSeparateCommits() {
			for (ModelType type : REF_DATA_TYPES) {
				var dTree = new Tree(database);
				timer.time("tree", () -> dTree.addBranch(type));
				timer.time("writer", () -> writer.commit(dTree, "Added data for type " + type.name()));
			}
		}

	}

	private static class Timer {

		private final Map<String, Long> times = new HashMap<>();

		private void time(String taskId, Runnable task) {
			long t = System.currentTimeMillis();
			task.run();
			t = System.currentTimeMillis() - t;
			long time = get(taskId);
			times.put(taskId, time + t);
		}

		private long get(String taskId) {
			if (times.containsKey(taskId))
				return times.get(taskId);
			return 0;
		}

		private void print(String taskId) {
			System.out.println("Task " + taskId + " took " + get(taskId) + "ms");
		}

	}

}
