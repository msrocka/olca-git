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
import org.openlca.git.commit.Committer;
import org.openlca.git.util.Diffs;

import com.google.common.io.Files;

public class Main {

	private static final String db = "ref_data";
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
			config.checkExisting = true;
			var writer = new DbWriter(config);

			if (repoDir.exists()) {
				delete(repoDir);
			}
			var olcaRepoDir = new File(database.getFileStorageLocation(), repoDir.getName());
			if (olcaRepoDir.exists()) {
				delete(olcaRepoDir);
			}
			repo.create(true);
			writer.refData(false);
			
			writer.update();
			writer.delete();
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
				ModelType.UNIT_GROUP,
				ModelType.FLOW_PROPERTY,
				ModelType.CURRENCY,
				ModelType.LOCATION,
				ModelType.DQ_SYSTEM
		};
		private final Config config;
		private final Committer writer;

		private DbWriter(Config config) {
			this.config = config;
			this.writer = new Committer(config);
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
			System.out.println(writer.commit(diffs, "Added data"));
		}

		private void refDataSeparateCommits() throws IOException {
			var diffs = Diffs.workspace(config);
			for (ModelType type : REF_DATA_TYPES) {
				var filtered = diffs.stream()
						.filter(d -> d.getNewPath().startsWith(type.name() + "/"))
						.collect(Collectors.toList());
				System.out.println(writer.commit(filtered, "Added data for type " + type.name()));
			}
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
			var writer = new Committer(config);
			System.out.println(writer.commit(diffs, "Updated data"));
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
			var writer = new Committer(config);
			System.out.println(writer.commit(diffs, "Deleted data"));
		}

	}

}
