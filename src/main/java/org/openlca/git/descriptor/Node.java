package org.openlca.git.descriptor;

import java.util.ArrayList;
import java.util.Collections;
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

	@Override
	public int compareTo(Node o) {
		return name.compareTo(o.name);
	}

}
