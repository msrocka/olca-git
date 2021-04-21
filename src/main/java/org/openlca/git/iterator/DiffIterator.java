package org.openlca.git.iterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

import org.eclipse.jgit.diff.DiffEntry;
import org.eclipse.jgit.errors.IncorrectObjectTypeException;
import org.eclipse.jgit.lib.FileMode;
import org.eclipse.jgit.lib.ObjectReader;
import org.eclipse.jgit.treewalk.AbstractTreeIterator;
import org.openlca.git.util.Diffs;
import org.openlca.util.Strings;

public class DiffIterator extends EntryIterator {

	private final List<DiffEntry> diffs;

	public DiffIterator(List<DiffEntry> diffs) {
		super(initialize(null, diffs));
		this.diffs = diffs;
	}

	private DiffIterator(DiffIterator parent, List<DiffEntry> diffs) {
		super(parent, initialize(parent.getEntryPathString(), diffs));
		this.diffs = diffs;
	}

	public DiffIterator(String prefix, List<DiffEntry> diffs) {
		super(prefix, initialize(prefix, diffs));
		this.diffs = diffs;
	}

	private static List<TreeEntry> initialize(String prefix, List<DiffEntry> diffs) {
		var list = new ArrayList<TreeEntry>();
		var added = new HashSet<String>();
		diffs.forEach(d -> {
			var path = Diffs.getPath(d);
			if (!Strings.nullOrEmpty(prefix)) {
				path = path.substring(prefix.length() + 1);
			}
			var name = path.contains("/") ? path.substring(0, path.indexOf('/')) : path;
			if (added.contains(name))
				return;
			if (path.contains("/")) {
				list.add(new TreeEntry(name, FileMode.TREE));
			} else {
				list.add(new TreeEntry(name, FileMode.REGULAR_FILE, d));
			}
			added.add(name);
		});
		return list;
	}

	@Override
	public AbstractTreeIterator createSubtreeIterator(ObjectReader reader)
			throws IncorrectObjectTypeException, IOException {
		return new DiffIterator(this, diffs.stream()
				.filter(d -> Diffs.getPath(d).startsWith(getEntryPathString() + "/"))
				.collect(Collectors.toList()));
	}

}
