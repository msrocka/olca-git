package org.openlca.git.repo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.eclipse.jgit.lib.FileMode;
import org.eclipse.jgit.lib.ObjectId;
import org.openlca.core.model.descriptors.Descriptor;
import org.openlca.git.descriptor.Node;

class TreeEntries {

	private Map<String, TreeEntry> entries = new HashMap<>();

	void sync(String name, FileMode fileMode, ObjectId objectId) {
		var entry = getEntry(name, fileMode);
		entry.objectId = objectId;
	}

	void sync(String name, FileMode fileMode, Object newContent) {
		var entry = getEntry(name, fileMode);
		entry.newContent = newContent;
	}

	private TreeEntry getEntry(String name, FileMode fileMode) {
		var entry = entries.get(name);
		if (entry != null)
			return entry;
		entry = new TreeEntry(name, fileMode);
		entries.put(name, entry);
		return entry;
	}

	boolean hasNewEntries() {
		for (TreeEntry entry : entries.values())
			if (entry.newContent != null)
				return true;
		return false;
	}
	
	List<TreeEntry> get(FileMode fileMode) {
		List<TreeEntry> values = new ArrayList<>(entries.values());
		Collections.sort(values);
		return values.stream().filter(e -> e.fileMode == fileMode).collect(Collectors.toList());
	}
	
	boolean isEmpty() {
		return entries.isEmpty();
	}

	class TreeEntry implements Comparable<TreeEntry> {

		final String name;
		final FileMode fileMode;
		ObjectId objectId;
		Object newContent;

		TreeEntry(String name, FileMode fileMode) {
			this.name = name;
			this.fileMode = fileMode;
		}

		Descriptor getDescriptor() {
			if (fileMode == FileMode.TREE)
				return null;
			return (Descriptor) newContent;
		}

		Node getNode() {
			if (fileMode == FileMode.REGULAR_FILE)
				return null;
			return (Node) newContent;
		}

		@Override
		public int compareTo(TreeEntry e) {
			return (getType() + name).compareTo(e.getType() + e.name);
		}

		private String getType() {
			return fileMode == FileMode.TREE ? "a" : "b";
		}

	}

}