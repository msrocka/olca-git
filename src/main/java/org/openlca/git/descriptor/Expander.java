package org.openlca.git.descriptor;

import java.util.List;

import org.openlca.core.database.CategoryDao;
import org.openlca.core.database.IDatabase;
import org.openlca.core.model.Category;
import org.openlca.core.model.ModelType;

class Expander {

	private final IDatabase database;
	private final Index index;

	Expander(IDatabase database, Index index) {
		this.database = database;
		this.index = index;
	}

	Node expand(ModelType type) {
		var root = new Node(type.name(), index.get(0));
		var rootCategories = new CategoryDao(database).getRootCategories(type);
		expand(root, rootCategories);
		root.sort();
		return root;
	}

	private void expand(Node parent, List<Category> categories) {
		for (var category : categories) {
			var content = index.get(category.id);
			var childCategories = category.childCategories;
			if ((content == null || content.isEmpty()) && childCategories.isEmpty())
				continue;
			var child = new Node(category.name, content);
			parent.children.add(child);
			if (childCategories.isEmpty())
				continue;
			expand(child, childCategories);
		}
	}

}