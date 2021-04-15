package org.openlca.git.repo;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import org.eclipse.jgit.lib.CommitBuilder;
import org.eclipse.jgit.lib.FileMode;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.ObjectInserter;
import org.eclipse.jgit.lib.TreeFormatter;
import org.eclipse.jgit.revwalk.RevWalk;
import org.eclipse.jgit.treewalk.TreeWalk;
import org.openlca.core.model.descriptors.Descriptor;
import org.openlca.git.Config;
import org.openlca.git.descriptor.Node;
import org.openlca.git.descriptor.Tree;
import org.openlca.git.repo.TreeEntries.TreeEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.thavam.util.concurrent.blockingMap.BlockingHashMap;
import org.thavam.util.concurrent.blockingMap.BlockingMap;

public class RepoWriter {

	private static final Logger log = LoggerFactory.getLogger(RepoWriter.class);
	private final Config config;
	private BlockingMap<String, byte[]> queue;
	private ExecutorService threads;

	public RepoWriter(Config config) {
		this.config = config;
	}

	public String commit(Tree dTree, String message) {
		if (dTree.isEmpty())
			return null;
		try {
			threads = Executors.newCachedThreadPool();
			queue = new BlockingHashMap<>();
			var previousCommitTreeId = getPreviousCommitTreeId();
			var entries = getEntries(previousCommitTreeId, dTree.getNodes(), null);
			var treeId = syncTree(entries);
			var commitId = commit(treeId, message);
			return commitId.name();
		} finally {
			queue.clear();
			threads.shutdown();
		}
	}

	private ObjectId syncTree(TreeEntries entries) {
		if (entries.isEmpty())
			return null;
		var tree = new TreeFormatter();
		syncChildren(tree, entries.get(FileMode.TREE));
		syncContent(tree, entries.get(FileMode.REGULAR_FILE));
		return insert(i -> i.insert(tree));
	}

	private void syncChildren(TreeFormatter tree, List<TreeEntry> branches) {
		if (branches.isEmpty())
			return;
		for (var branch : branches) {
			if (branch.fileMode != FileMode.TREE)
				continue;
			var entries = getEntries(branch.objectId, branch.getNode());
			var treeId = branch.objectId;
			if (entries.hasNewEntries()) {
				treeId = syncTree(entries);
			}
			tree.append(branch.name, FileMode.TREE, treeId);
		}
	}

	private void syncContent(TreeFormatter tree, List<TreeEntry> entries) {
		if (entries.isEmpty())
			return;
		var inserter = new Inserter(config, queue, threads);
		var converter = new Converter(config, queue, threads);
		var writer = inserter.insert(entries, tree);
		converter.convert(entries.stream().filter(e -> e.newContent != null).collect(Collectors.toList()));
		waitFor(writer);
	}

	private TreeEntries getEntries(ObjectId treeId, Node node) {
		return getEntries(treeId, node != null ? node.children : null, node != null ? node.content : null);
	}

	private TreeEntries getEntries(ObjectId treeId, List<Node> children, List<Descriptor> content) {
		var entries = new TreeEntries();
		if (treeId != null) {
			try (var walk = new TreeWalk(config.repo)) {
				walk.addTree(treeId);
				while (walk.next()) {
					entries.sync(walk.getNameString(), walk.getFileMode(), walk.getObjectId(0));
				}
			} catch (IOException e) {
				log.error("Error walking tree " + treeId, e);
			}
		}
		if (children != null) {
			for (var child : children) {
				entries.sync(child.name, FileMode.TREE, child);
			}
		}
		if (content != null) {
			for (var descriptor : content) {
				var name = descriptor.refId + (config.asProto ? ".proto" : ".json");
				entries.sync(name, FileMode.REGULAR_FILE, descriptor);
			}
		}
		return entries;
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

	private void waitFor(Future<?> future) {
		try {
			future.get();
		} catch (Exception e) {
			log.error("failed to finish a thread", e);
		}
	}

	private interface Insert {

		ObjectId insertInto(ObjectInserter inserter) throws IOException;

	}
}
