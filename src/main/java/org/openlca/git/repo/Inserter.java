package org.openlca.git.repo;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.eclipse.jgit.internal.storage.file.PackInserter;
import org.eclipse.jgit.lib.Constants;
import org.eclipse.jgit.lib.FileMode;
import org.eclipse.jgit.lib.TreeFormatter;
import org.openlca.git.Config;
import org.openlca.git.repo.TreeEntries.TreeEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.thavam.util.concurrent.blockingMap.BlockingMap;

class Inserter {

	private static final Logger log = LoggerFactory.getLogger(Inserter.class);
	private final Config config;
	private final BlockingMap<String, byte[]> queue;
	private final ExecutorService threads;

	Inserter(Config config, BlockingMap<String, byte[]> queue, ExecutorService threads) {
		this.config = config;
		this.queue = queue;
		this.threads = threads;
	}

	public Future<?> insert(List<TreeEntry> entries, TreeFormatter tree) {
		return threads.submit(() -> {
			var inserter = config.repo.getObjectDatabase().newPackInserter();
			inserter.checkExisting(config.checkExisting);
			for (var entry : entries) {
				write(entry, tree, inserter);
			}
			try {
				inserter.flush();
				inserter.close();
			} catch (IOException e) {
				log.error("failed to flush objects", e);
			}
		});
	}

	private void write(TreeEntry entry, TreeFormatter tree, PackInserter inserter) {
		if (entry.fileMode != FileMode.REGULAR_FILE)
			return;
		try {
			var blobId = entry.objectId;
			if (blobId == null) {
				var data = queue.take(entry.name);
				if (data == null)
					return;
				blobId = inserter.insert(Constants.OBJ_BLOB, data);
			}
			tree.append(entry.name, FileMode.REGULAR_FILE, blobId);
		} catch (Exception e) {
			log.error("failed to write data set", e);
		}
	}

}