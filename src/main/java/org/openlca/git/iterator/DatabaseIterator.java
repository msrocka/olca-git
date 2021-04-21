package org.openlca.git.iterator;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.eclipse.jgit.errors.IncorrectObjectTypeException;
import org.eclipse.jgit.lib.ObjectReader;
import org.eclipse.jgit.treewalk.AbstractTreeIterator;
import org.openlca.core.database.CategoryDao;
import org.openlca.core.database.Daos;
import org.openlca.core.model.Category;
import org.openlca.core.model.ModelType;
import org.openlca.core.model.descriptors.CategorizedDescriptor;
import org.openlca.core.model.descriptors.Descriptor;
import org.openlca.git.Config;

public class DatabaseIterator extends EntryIterator {

	private final Config config;

	public DatabaseIterator(Config config) {
		super(init(config));
		this.config = config;
	}

	private static List<TreeEntry> init(Config config) {
		return Arrays.asList(ModelType.categorized())
				.stream().filter(type -> {
					var dao = new CategoryDao(config.database);
					if (type == ModelType.CATEGORY)
						return false;
					if (!dao.getRootCategories(type).isEmpty())
						return true;
					return !Daos.categorized(config.database, type).getDescriptors(Optional.empty()).isEmpty();
				}).map(type -> new TreeEntry(type))
				.collect(Collectors.toList());
	}

	public DatabaseIterator(Config config, ModelType type) {
		super(type.name(), init(config, type));
		this.config = config;
	}

	private DatabaseIterator(DatabaseIterator parent, Config config, ModelType type) {
		super(parent, init(config, type));
		this.config = config;
	}

	private static List<TreeEntry> init(Config config, ModelType type) {
		var entries = new CategoryDao(config.database).getRootCategories(type)
				.stream().map(c -> new TreeEntry(c))
				.collect(Collectors.toList());
		entries.addAll(Daos.categorized(config.database, type).getDescriptors(Optional.empty())
				.stream().map(d -> new TreeEntry(d, config.asProto))
				.collect(Collectors.toList()));
		return entries;
	}

	public DatabaseIterator(Config config, Category category) {
		super(toPath(category), init(config, category));
		this.config = config;
	}

	private static String toPath(Category category) {
		if (category.category == null)
			return category.modelType.name() + "/" + category.name;
		return toPath(category.category) + "/" + category.name;
	}

	private DatabaseIterator(DatabaseIterator parent, Config config, Category category) {
		super(parent, init(config, category));
		this.config = config;
	}

	private static List<TreeEntry> init(Config config, Category category) {
		var entries = category.childCategories
				.stream().map(c -> new TreeEntry(c))
				.collect(Collectors.toList());
		entries.addAll(Daos.categorized(config.database, category.modelType).getDescriptors(Optional.of(category))
				.stream().map(d -> new TreeEntry(d, config.asProto))
				.collect(Collectors.toList()));
		return entries;
	}

	@Override
	public boolean hasId() {
		var e = getEntry();
		if (e.data instanceof ModelType)
			return config.store.has((ModelType) e.data);
		if (e.data instanceof Category)
			return config.store.has(Descriptor.of((Category) e.data));
		return config.store.has((CategorizedDescriptor) e.data);
	}

	@Override
	public byte[] idBuffer() {
		var e = getEntry();
		if (e.data instanceof ModelType)
			return config.store.getRaw((ModelType) e.data);
		if (e.data instanceof Category)
			return config.store.getRaw(Descriptor.of((Category) e.data));
		return config.store.getRaw((CategorizedDescriptor) e.data);
	}

	@Override
	public int idOffset() {
		return 0;
	}

	@Override
	public AbstractTreeIterator createSubtreeIterator(ObjectReader reader)
			throws IncorrectObjectTypeException, IOException {
		var entry = getEntry();
		if (entry.data instanceof ModelType)
			return new DatabaseIterator(this, config, (ModelType) entry.data);
		if (entry.data instanceof Category)
			return new DatabaseIterator(this, config, (Category) entry.data);
		return null;
	}

}
