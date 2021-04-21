package org.openlca.git.commit;

import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.eclipse.jgit.diff.DiffEntry;
import org.eclipse.jgit.diff.DiffEntry.ChangeType;
import org.eclipse.jgit.lib.FileMode;
import org.openlca.core.model.ModelType;
import org.openlca.git.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.thavam.util.concurrent.blockingMap.BlockingMap;

class Converter {

	private static final Logger log = LoggerFactory.getLogger(Converter.class);
	private final Config config;
	private final BlockingMap<String, byte[]> queue;
	private final ExecutorService threads;
	private Deque<DiffEntry> entries;

	Converter(Config config, BlockingMap<String, byte[]> queue, ExecutorService threads) {
		this.config = config;
		this.queue = queue;
		this.threads = threads;
	}

	void convert(List<DiffEntry> diffs) {
		this.entries = new LinkedList<>(diffs);
		var noOfThreads = config.converterThreads <= diffs.size()
				? config.converterThreads
				: diffs.size();
		synchronized (entries) {
			for (var i = 0; i < noOfThreads; i++) {
				startNext(entries.pop());
			}
		}
	}

	private void startNext(DiffEntry entry) {
		threads.submit(() -> {
			convert(entry);
			synchronized (entries) {
				if (!entries.isEmpty()) {
					startNext(entries.pop());
				}
			}
		});
	}

	private void convert(DiffEntry entry) {
		if (entry.getChangeType() == ChangeType.DELETE || entry.getNewMode() != FileMode.REGULAR_FILE)
			return;
		var path = entry.getNewPath();
		var type = ModelType.valueOf(path.substring(0, path.indexOf('/'))).getModelClass();
		var name = path.substring(path.lastIndexOf('/') + 1);
		var refId = name.substring(0, name.indexOf('.'));
		try {
			var model = config.database.get(type, refId);
			var data = ProtoWriter.convert(model, config);
			offer(path, data);
		} catch (Exception e) {
			log.error("failed to convert data set " + entry, e);
			offer(path, null);
		}
	}

	private void offer(String refId, byte[] data) {
		try {
			queue.offer(refId, data, 5, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			log.error("failed to add element to data queue", e);
		}
	}

}
