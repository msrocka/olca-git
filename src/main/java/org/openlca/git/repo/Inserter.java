package org.openlca.git.repo;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.eclipse.jgit.internal.storage.file.PackInserter;
import org.eclipse.jgit.lib.Constants;
import org.eclipse.jgit.lib.FileMode;
import org.eclipse.jgit.lib.TreeFormatter;
import org.openlca.core.model.descriptors.Descriptor;
import org.openlca.git.Config;
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

	public Future<?> insert(List<Descriptor> descriptors, TreeFormatter tree) {
		return threads.submit(() -> {
			var inserter = config.repo.getObjectDatabase().newPackInserter();
			inserter.checkExisting(config.checkExisting);
			for (var descriptor : descriptors) {
				write(descriptor, tree, inserter);
			}
			try {
				inserter.flush();
				inserter.close();
			} catch (IOException e) {
				log.error("failed to flush objects", e);
			}
		});
	}

	private void write(Descriptor descriptor, TreeFormatter tree, PackInserter inserter) {
		try {
			var data = queue.take(descriptor.refId);
			if (data == null)
				return;
			var blobId = inserter.insert(Constants.OBJ_BLOB, data);
			var extension = config.asProto ? ".proto" : ".json";
			var name = descriptor.refId + extension;
			tree.append(name, FileMode.REGULAR_FILE, blobId);
		} catch (Exception e) {
			log.error("failed to write data set", e);
		}
	}

}