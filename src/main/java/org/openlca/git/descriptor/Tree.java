package org.openlca.git.descriptor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Set;

import org.openlca.core.database.Daos;
import org.openlca.core.database.IDatabase;
import org.openlca.core.model.ModelType;

public class Tree {

	private final IDatabase database;
	private final EnumMap<ModelType, Node> roots;

	public Tree(IDatabase database) {
		this.database = database;
		roots = new EnumMap<>(ModelType.class);
	}

	public void addBranch(ModelType type) {
		addBranch(type, null);
	}

	public void addBranch(ModelType type, Set<String> refIds) {
		var dao = Daos.root(database, type);
		var descriptors = refIds != null
				? dao.getDescriptorsForRefIds(refIds)
				: dao.getDescriptors();
		var index = new Index(descriptors);
		if (index.isEmpty())
			return;
		var expander = new Expander(database, index);
		roots.put(type, expander.expand(type));
	}

	public List<Node> getNodes() {
		var nodes = new ArrayList<>(roots.values());
		Collections.sort(nodes);
		return nodes;
	}

	public boolean isEmpty() {
		for (var node : roots.values())
			if (!node.content.isEmpty() || !node.children.isEmpty())
				return false;
		return true;
	}

}
