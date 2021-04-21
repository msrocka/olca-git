package org.openlca.git.util;

import java.io.IOException;
import java.util.List;

import org.eclipse.jgit.diff.DiffEntry;
import org.eclipse.jgit.diff.DiffEntry.ChangeType;
import org.eclipse.jgit.lib.Constants;
import org.eclipse.jgit.treewalk.EmptyTreeIterator;
import org.eclipse.jgit.treewalk.TreeWalk;
import org.openlca.git.Config;
import org.openlca.git.iterator.DatabaseIterator;

public class Diffs {

	public static List<DiffEntry> workspace(Config config) throws IOException {
		var walk = new TreeWalk(config.repo);
		var head = config.repo.resolve(Constants.HEAD);
		if (head == null) {
			walk.addTree(new EmptyTreeIterator());
		} else {
			var commit = config.repo.parseCommit(head);
			walk.addTree(commit.getTree().getId());

		}
		walk.addTree(new DatabaseIterator(config));
		walk.setRecursive(true);
		return DiffEntry.scan(walk);
	}

	public static boolean matches(DiffEntry d, String path) {
		if (d == null)
			return false;
		return path != null && getPath(d).equals(path);
	}

	public static boolean matches(DiffEntry d, ChangeType type) {
		if (d == null)
			return false;
		return type != null && d.getChangeType() == type;
	}

	public static String getPath(DiffEntry d) {
		return d.getChangeType() == ChangeType.DELETE ? d.getOldPath() : d.getNewPath();
	}

}
