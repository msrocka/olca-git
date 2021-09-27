package org.openlca.git;

import java.io.File;
import java.io.IOException;
import java.util.UUID;
import java.util.stream.Collectors;

import org.eclipse.jgit.internal.storage.file.FileRepository;
import org.eclipse.jgit.lib.PersonIdent;
import org.openlca.core.DataDir;
import org.openlca.core.database.CurrencyDao;
import org.openlca.core.database.Daos;
import org.openlca.core.database.Derby;
import org.openlca.core.database.LocationDao;
import org.openlca.core.model.Location;
import org.openlca.core.model.ModelType;
import org.openlca.core.model.descriptors.Descriptor;
import org.openlca.git.commit.CommitWriter;
import org.openlca.git.util.Diffs;
import org.openlca.git.util.GitUtil;

import com.google.common.io.Files;

public class Main {

	 private static final String db = "ecoinvent_36_cutoff_lci_20200206";
//	private static final String db = "ecoinvent_36_cutoff_unit_20200512";
	private static final PersonIdent committer = new PersonIdent("greve", "greve@greendelta.com");
	private static final File repoDir = new File("C:/Users/Sebastian/test/olca-git/" + db);
	private static final File tmp = new File("C:/Users/Sebastian/test/tmp");

	public static void main(String[] args) throws IOException {
		var dbDir = new File(DataDir.databases(), db);
		if (tmp.exists()) {
			delete(tmp);
		}
		copy(dbDir, tmp);
		try (var database = new Derby(tmp);
				var repo = new FileRepository(repoDir)) {
			var config = Config.newJsonConfig(database, repo, committer);
			config.checkExisting = false;
			var writer = new DbWriter(config);

			if (repoDir.exists()) {
				delete(repoDir);
			}
			var olcaRepoDir = new File(database.getFileStorageLocation(),
					repoDir.getName());
			if (olcaRepoDir.exists()) {
				delete(olcaRepoDir);
			}
			repo.create(true);
			writer.refData(false);

			// writer.update();
			// writer.delete();
		}
	}

	private static void copy(File from, File to) throws IOException {
		if (from.isDirectory()) {
			to.mkdirs();
			for (File child : from.listFiles()) {
				copy(child, new File(to, child.getName()));
			}
			return;
		}
		Files.copy(from, to);
	}

	private static void delete(File file) {
		if (file.isDirectory()) {
			for (File child : file.listFiles()) {
				delete(child);
			}
		}
		file.delete();
	}

	private static class DbWriter {

		private static final ModelType[] REF_DATA_TYPES = {
				ModelType.ACTOR,
				ModelType.SOURCE,
				ModelType.UNIT_GROUP,
				ModelType.FLOW_PROPERTY,
				ModelType.CURRENCY,
				ModelType.LOCATION,
				ModelType.DQ_SYSTEM,
				ModelType.FLOW,
				ModelType.PROCESS,
				ModelType.IMPACT_CATEGORY,
				ModelType.IMPACT_METHOD
		};
		private final Config config;
		private final CommitWriter writer;

		private DbWriter(Config config) {
			this.config = config;
			this.writer = new CommitWriter(config);
		}

		private void refData(boolean singleCommit) throws IOException {
			if (singleCommit) {
				refDataSingleCommit();
			} else {
				refDataSeparateCommits();
			}
		}

		private void refDataSingleCommit() throws IOException {
			var diffs = Diffs.workspace(config);
			System.out.println(writer.commit("Added data", diffs));
		}

		private void refDataSeparateCommits() throws IOException {
			var diffs = Diffs.workspace(config);
			long time = 0;
			for (ModelType type : REF_DATA_TYPES) {
				var filtered = diffs.stream()
						.filter(d -> d.getNewPath().startsWith(type.name() + "/"))
						.collect(Collectors.toList());
				long t = System.currentTimeMillis();
				System.out.println("Committing " + filtered.size() + " files");
				System.out.println(writer.commit("Added data for type " + type.name(), filtered));
				time += System.currentTimeMillis() - t;
			}
			System.out.println("Total time: " + time + "ms");
		}

		private void update() throws IOException {
			var dao = new LocationDao(config.database);

			var deleted = dao.getAll().get(5);
			dao.delete(deleted);
			config.store.invalidate(Descriptor.of(deleted));

			var changed = dao.getAll().get(0);
			changed.description = "changed " + Math.random();
			dao.update(changed);
			config.store.invalidate(Descriptor.of(changed));

			var newLoc = new Location();
			newLoc.refId = UUID.randomUUID().toString();
			newLoc.name = "new";
			dao.insert(newLoc);
			config.store.save();

			var diffs = Diffs.workspace(config);
			var writer = new CommitWriter(config);
			System.out.println(writer.commit("Updated data", diffs));
		}

		private void delete() throws IOException {
			for (var type : REF_DATA_TYPES) {
				for (var d : Daos.categorized(config.database, type).getDescriptors()) {
					config.store.invalidate(d);
				}
				if (type == ModelType.CURRENCY) {
					var dao = new CurrencyDao(config.database);
					var currencies = dao.getAll();
					for (var currency : currencies) {
						currency.referenceCurrency = null;
						dao.update(currency);
					}
					dao.deleteAll();
				} else {
					Daos.categorized(config.database, type).deleteAll();
				}
			}
			config.store.save();
			var diffs = Diffs.workspace(config);
			var writer = new CommitWriter(config);
			System.out.println(writer.commit("Deleted data", diffs));
		}

	}

}
