package org.openlca.git;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;

import org.openlca.core.database.CategoryDao;
import org.openlca.core.database.IDatabase;
import org.openlca.core.model.CategorizedEntity;
import org.openlca.core.model.Category;
import org.openlca.core.model.ModelType;
import org.openlca.core.model.descriptors.CategorizedDescriptor;
import org.openlca.core.model.descriptors.CategoryDescriptor;
import org.openlca.core.model.descriptors.Descriptor;
import org.openlca.util.Pair;

import gnu.trove.map.hash.TLongObjectHashMap;

class DbTree {

  static class Node {
    final String name;
    final List<Descriptor> content;
    final List<Node> childs = new ArrayList<>();

    private Node(String name, List<Descriptor> content) {
      this.name = name;
      this.content = content == null
        ? Collections.emptyList()
        : content;
    }
  }

  private final EnumMap<ModelType, Node> roots;

  private DbTree() {
    this.roots = new EnumMap<>(ModelType.class);
  }

  Node getRoot(ModelType type) {
    return roots.get(type);
  }

  static DbTree build(IDatabase db) {
    var tree = new DbTree();
    for (var type : ModelType.values()) {
      var c = type.getModelClass();
      if (type == ModelType.CATEGORY
        || c == null
        || !CategorizedEntity.class.isAssignableFrom(c))
        continue;
      tree.addBranch(type, db);
    }
    return tree;
  }

  private void addBranch(ModelType type, IDatabase db) {
    var all = db.allDescriptorsOf(type.getModelClass());
    if (all.isEmpty())
      return;

    // index the descriptors by category
    var index = new TLongObjectHashMap<List<Descriptor>>();
    for (var d : all) {
      if (!(d instanceof CategorizedDescriptor))
        continue;
      var cd = (CategorizedDescriptor) d;
      long catID = cd.category == null
        ? 0
        : cd.category;
      var list = index.get(catID);
      if (list == null) {
        list = new ArrayList<>();
        index.put(catID, list);
      }
      list.add(cd);
    }
    if (index.isEmpty())
      return;

    // expand the nodes recursively
    var root = new Node(type.name(), index.get(0));
    roots.put(type, root);
    var rootCategories = new CategoryDao(db)
      .getRootCategories(type);
    var queue = new ArrayDeque<Pair<Node, List<Category>>>();
    queue.add(Pair.of(root, rootCategories));
    while (!queue.isEmpty()) {
      var next = queue.poll();
      var parent = next.first;
      for (var category : next.second) {
        var content = index.get(category.id);
        var childCategories = category.childCategories;
        if (content == null
          || content.isEmpty()
          || childCategories.isEmpty())
          continue;
        var child = new Node(category.name, content);
        parent.childs.add(child);
        if (!childCategories.isEmpty()) {
          queue.add(Pair.of(child, childCategories));
        }
      }
    }
  }

}
