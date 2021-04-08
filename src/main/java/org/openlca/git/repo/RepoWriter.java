package org.openlca.git.repo;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.eclipse.jgit.lib.CommitBuilder;
import org.eclipse.jgit.lib.FileMode;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.ObjectInserter;
import org.eclipse.jgit.lib.TreeFormatter;
import org.openlca.core.model.descriptors.Descriptor;
import org.openlca.git.Config;
import org.openlca.git.descriptor.Node;
import org.openlca.git.descriptor.Tree;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.thavam.util.concurrent.blockingMap.BlockingHashMap;

public class RepoWriter {

	private static final Logger log = LoggerFactory.getLogger(RepoWriter.class);
	private final Config config;

	public RepoWriter(Config config) {
		this.config = config;
	}

	public void commit(Tree dTree, String message) {
		if (dTree.isEmpty())
			return;
		var tree = new TreeFormatter();
		appendChildren(tree, dTree.getNodes());
		var commitId = commit(tree, message);
		updateHeadWith(commitId);
	}

	private ObjectId createTree(Node node) {
		if (node.content.isEmpty() && node.children.isEmpty())
			return null;
		var tree = new TreeFormatter();
		appendChildren(tree, node.children);
		if (!node.content.isEmpty()) {
			appendContent(tree, node.content);
		}
		return insert(i -> i.insert(tree));
	}

	private void appendChildren(TreeFormatter tree, List<Node> nodes) {
		for (var node : nodes) {
			var treeId = createTree(node);
			if (treeId == null)
				continue;
			tree.append(node.name, FileMode.TREE, treeId);
		}
	}

	private void appendContent(TreeFormatter tree, List<Descriptor> descriptors) {
		var queue = new BlockingHashMap<String, byte[]>();
		var threads = Executors.newCachedThreadPool();
		try {
			var inserter = new Inserter(config, queue, threads);
			var converter = new Converter(config, queue, threads);
			var writer = inserter.insert(descriptors, tree);
			converter.convert(descriptors);
			waitFor(writer);
		} finally {
			threads.shutdown();
		}
	}

	private ObjectId commit(TreeFormatter tree, String message) {
		var treeId = insert(i -> i.insert(tree));
		var commit = new CommitBuilder();
		commit.setAuthor(config.committer);
		commit.setCommitter(config.committer);
		commit.setMessage(message);
		commit.setEncoding(StandardCharsets.UTF_8);
		commit.setTreeId(treeId);
		return insert(i -> i.insert(commit));
	}

	private void updateHeadWith(ObjectId commitId) {
		try {
			var ref = config.repo.findRef("HEAD");
			var update = config.repo.updateRef(ref.getName());
			update.setNewObjectId(commitId);
			update.update();
		} catch (IOException e) {
			log.error("failed to update head", e);
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
