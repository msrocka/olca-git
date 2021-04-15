package org.openlca.git.repo;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.eclipse.jgit.lib.FileMode;
import org.openlca.git.Config;
import org.openlca.git.repo.TreeEntries.TreeEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.thavam.util.concurrent.blockingMap.BlockingMap;

class Converter {

	private static final Logger log = LoggerFactory.getLogger(Converter.class);
	private static final int workerCount = 8;
	private final Config config;
	private final BlockingMap<String, byte[]> queue;
	private final ExecutorService threads;

	Converter(Config config, BlockingMap<String, byte[]> queue, ExecutorService threads) {
		this.config = config;
		this.queue = queue;
		this.threads = threads;
	}

	void convert(List<TreeEntry> entries) {
		var futures = new ArrayList<Future<?>>(workerCount);
		var total = entries.size();
		var offset = 0;
		while (offset < total) {
			for (var i = 0; i < config.converterThreads; i++) {
				if (offset >= total)
					break;
				var descriptor = entries.get(offset);
				var future = threads.submit(() -> convert(descriptor));
				futures.add(future);
				offset++;
			}
			if (futures.isEmpty())
				break;
			waitFor(futures);
		}
	}

	private void convert(TreeEntry entry) {
		if (entry.fileMode != FileMode.REGULAR_FILE)
			return;
		var descriptor = entry.getDescriptor();
		try {
			var model = config.database.get(descriptor.type.getModelClass(), descriptor.id);
			var data = ProtoWriter.convert(model, config);
			offer(entry.name, data);
		} catch (Exception e) {
			log.error("failed to convert data set " + entry, e);
			offer(entry.name, null);
		}
	}

	private void offer(String refId, byte[] data) {
		try {
			queue.offer(refId, data, 5, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			log.error("failed to add element to data queue", e);
		}
	}

	private void waitFor(List<Future<?>> futures) {
		try {
			for (var future : futures) {
				try {
					future.get();
				} catch (Exception e) {
					log.error("failed to finish a thread", e);
				}
			}
		} finally {
			futures.clear();
		}
	}

}
