package org.openlca.git.descriptor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.openlca.core.model.descriptors.Descriptor;

public class Node implements Comparable<Node> {

	public final String name;
	public final List<Descriptor> content;
	public final List<Node> children = new ArrayList<>();

	public Node(String name, List<Descriptor> content) {
		this.name = name;
		this.content = content == null ? Collections.emptyList() : content;
	}

	void sort() {
		sort(this);
	}

	private void sort(Node node) {
		Collections.sort(node.content, new DescriptorSort());
		Collections.sort(node.children);
		for (var child : node.children) {
			sort(child);
		}
	}

	@Override
	public int compareTo(Node o) {
		return name.compareTo(o.name);
	}

	private static class DescriptorSort implements Comparator<Descriptor> {

		@Override
		public int compare(Descriptor o1, Descriptor o2) {
			return o1.refId.compareTo(o2.refId);
		}

	}

}
