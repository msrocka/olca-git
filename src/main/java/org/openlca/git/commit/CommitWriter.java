package org.openlca.git.commit;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import org.eclipse.jgit.diff.DiffEntry;
import org.eclipse.jgit.diff.DiffEntry.ChangeType;
import org.eclipse.jgit.internal.storage.file.PackInserter;
import org.eclipse.jgit.lib.CommitBuilder;
import org.eclipse.jgit.lib.Constants;
import org.eclipse.jgit.lib.FileMode;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.ObjectInserter;
import org.eclipse.jgit.lib.TreeFormatter;
import org.eclipse.jgit.revwalk.RevWalk;
import org.eclipse.jgit.treewalk.CanonicalTreeParser;
import org.eclipse.jgit.treewalk.EmptyTreeIterator;
import org.eclipse.jgit.treewalk.TreeWalk;
import org.openlca.git.Config;
import org.openlca.git.iterator.DiffIterator;
import org.openlca.git.util.Diffs;
import org.openlca.git.util.ObjectIds;
import org.openlca.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.thavam.util.concurrent.blockingMap.BlockingHashMap;
import org.thavam.util.concurrent.blockingMap.BlockingMap;

public class CommitWriter {

	private static final Logger log = LoggerFactory.getLogger(CommitWriter.class);
	private final Config config;
	private BlockingMap<String, byte[]> queue;
	private PackInserter inserter;

	public CommitWriter(Config config) {
		this.config = config;
	}

	public String commit(String message, List<DiffEntry> diffs) throws IOException {
		if (diffs.isEmpty())
			return null;
		var threads = Executors.newCachedThreadPool();
		queue = new BlockingHashMap<>();
		try {
			inserter = config.repo.getObjectDatabase().newPackInserter();
			inserter.checkExisting(config.checkExisting);
			new Converter(config, queue, threads).convert(diffs.stream()
					.filter(d -> d.getChangeType() != ChangeType.DELETE)
					.collect(Collectors.toList()));
			var previousCommitTreeId = getPreviousCommitTreeId();
			var treeId = syncTree("", previousCommitTreeId, diffs);
			config.store.putRoot(treeId);
			config.store.save();
			var commitId = commit(treeId, message);
			return commitId.name();
		} finally {
			if (inserter != null) {
				inserter.flush();
				inserter.close();
			}
			queue.clear();
			threads.shutdown();
		}
	}

	private ObjectId syncTree(String prefix, ObjectId treeId, List<DiffEntry> diffs) {
		boolean appended = false;
		var tree = new TreeFormatter();
		try (var walk = createWalk(prefix, treeId, diffs)) {
			while (walk.next()) {
				var mode = walk.getFileMode();
				var name = walk.getNameString();
				ObjectId id = null;
				if (mode == FileMode.TREE) {
					id = handleTree(walk, diffs);
				} else if (mode == FileMode.REGULAR_FILE) {
					id = handleFile(walk);
				}
				if (ObjectIds.isNullOrZero(id))
					continue;
				tree.append(name, mode, id);
				appended = true;
			}
		} catch (Exception e) {
			log.error("Error walking tree " + treeId, e);
		}
		if (!appended && !Strings.nullOrEmpty(prefix))
			return null;
		return insert(i -> i.insert(tree));
	}

	private TreeWalk createWalk(String prefix, ObjectId treeId, List<DiffEntry> diffs) throws IOException {
		var walk = new TreeWalk(config.repo);
		if (treeId == null || treeId.equals(ObjectId.zeroId())) {
			walk.addTree(new EmptyTreeIterator());
		} else if (Strings.nullOrEmpty(prefix)) {
			walk.addTree(treeId);
		} else {
			walk.addTree(new CanonicalTreeParser(prefix.getBytes(), walk.getObjectReader(), treeId));
		}
		var diffIterator = Strings.nullOrEmpty(prefix)
				? new DiffIterator(diffs)
				: new DiffIterator(prefix, diffs);
		walk.addTree(diffIterator);
		return walk;
	}

	private ObjectId handleTree(TreeWalk walk, List<DiffEntry> diffs) {
		var treeId = walk.getObjectId(0);
		if (walk.getFileMode(1) == FileMode.MISSING)
			return treeId;
		var prefix = walk.getPathString();
		return syncTree(prefix, treeId, diffs.stream()
				.filter(d -> Strings.nullOrEmpty(prefix) || Diffs.getPath(d).startsWith(prefix))
				.collect(Collectors.toList()));
	}

	private ObjectId handleFile(TreeWalk walk)
			throws IOException, InterruptedException {
		var blobId = walk.getObjectId(0);
		if (walk.getFileMode(1) == FileMode.MISSING)
			return blobId;
		var path = walk.getPathString();
		DiffEntry diff = walk.getTree(1, DiffIterator.class).getEntryData();
		boolean matchesPath = Diffs.matches(diff, path);
		if (matchesPath && diff.getChangeType() == ChangeType.DELETE)
			return null;
		var data = queue.take(path);
		if (diff.getChangeType() == ChangeType.MODIFY && ObjectIds.equal(data, blobId))
			return blobId;
		blobId = inserter.insert(Constants.OBJ_BLOB, data);
		config.store.put(path, blobId);
		return blobId;
	}

	private ObjectId getPreviousCommitTreeId() {
		try (var walk = new RevWalk(config.repo)) {
			var head = config.repo.resolve("refs/heads/master");
			if (head == null)
				return null;
			var commit = walk.parseCommit(head);
			if (commit == null)
				return null;
			return commit.getTree().getId();
		} catch (IOException e) {
			log.error("Error reading commit tree", e);
			return null;
		}
	}

	private ObjectId commit(ObjectId treeId, String message) {
		try {
			var commit = new CommitBuilder();
			commit.setAuthor(config.committer);
			commit.setCommitter(config.committer);
			commit.setMessage(message);
			commit.setEncoding(StandardCharsets.UTF_8);
			commit.setTreeId(treeId);
			var head = config.repo.findRef("HEAD");
			var previousCommitId = head.getObjectId();
			if (previousCommitId != null) {
				commit.addParentId(previousCommitId);
			}
			var commitId = insert(i -> i.insert(commit));
			var update = config.repo.updateRef(head.getName());
			update.setNewObjectId(commitId);
			update.update();
			return commitId;
		} catch (IOException e) {
			log.error("failed to update head", e);
			return null;
		}
	}

	private ObjectId insert(Insert insertion) {
		try (var inserter = config.repo.newObjectInserter()) {
			return insertion.insertInto(inserter);
		} catch (IOException e) {
			log.error("failed to insert", e);
			return null;
		}
	}

	private interface Insert {

		ObjectId insertInto(ObjectInserter inserter) throws IOException;

	}

}