package org.openlca.git;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Map;

import org.eclipse.jgit.lib.ObjectId;
import org.openlca.core.database.IDatabase;
import org.openlca.core.model.ModelType;
import org.openlca.core.model.descriptors.CategorizedDescriptor;
import org.openlca.core.model.descriptors.CategoryDescriptor;
import org.openlca.util.CategoryPathBuilder;

public class ObjectIdStore {

	private final File file;
	private final boolean asProto;
	private final CategoryPathBuilder categoryPath;
	private Map<String, byte[]> store = new HashMap<>();

	private ObjectIdStore(IDatabase database, String repository, boolean asProto) {
		var repoDir = new File(database.getFileStorageLocation(), repository);
		this.file = new File(repoDir, "object-id.store");
		this.categoryPath = new CategoryPathBuilder(database);
		this.asProto = asProto;
	}

	public static ObjectIdStore openProto(IDatabase database, String repository) throws IOException {
		var store = new ObjectIdStore(database, repository, true);
		store.load();
		return store;
	}

	public static ObjectIdStore openJson(IDatabase database, String repository) throws IOException {
		var store = new ObjectIdStore(database, repository, false);
		store.load();
		return store;
	}

	@SuppressWarnings("unchecked")
	private void load() throws IOException {
		if (!file.exists())
			return;
		try (var fis = new FileInputStream(file);
				var ois = new ObjectInputStream(fis)) {
			store = (HashMap<String, byte[]>) ois.readObject();
		} catch (ClassNotFoundException e) {
			throw new IOException(e);
		}
	}

	public void save() throws IOException {
		if (!file.getParentFile().exists()) {
			file.getParentFile().mkdirs();
		}
		try (var fos = new FileOutputStream(file);
				var oos = new ObjectOutputStream(fos)) {
			oos.writeObject(store);
		}
	}

	public boolean has(ModelType type) {
		var key = getKey(type, null, null);
		return has(key);
	}

	public boolean has(CategorizedDescriptor d) {
		var key = getKey(d);
		return has(key);
	}

	private boolean has(String key) {
		return store.containsKey(key);
	}

	public byte[] getRawRoot() {
		return getRaw("");
	}

	public byte[] getRaw(ModelType type) {
		var key = getKey(type, null, null);
		return getRaw(key);
	}

	public byte[] getRaw(CategorizedDescriptor d) {
		var key = getKey(d);
		return getRaw(key);
	}

	private byte[] getRaw(String key) {
		var v = store.get(key);
		if (v == null)
			return getBytes(ObjectId.zeroId());
		return v;
	}

	public ObjectId getRoot() {
		return get("");
	}

	public ObjectId get(ModelType type) {
		var key = getKey(type, null, null);
		return get(key);
	}

	public ObjectId get(CategorizedDescriptor d) {
		var key = getKey(d);
		return get(key);
	}

	private ObjectId get(String key) {
		var id = store.get(key);
		if (id == null)
			return null;
		return ObjectId.fromRaw(id);
	}

	public void putRoot(ObjectId id) {
		put("", id);
	}

	public void put(ModelType type, ObjectId id) {
		var key = getKey(type, null, null);
		put(key, id);
	}

	public void put(CategorizedDescriptor d, ObjectId id) {
		var key = getKey(d);
		put(key, id);
	}

	public void put(String key, ObjectId id) {
		if (key.startsWith("/")) {
			key = key.substring(1);
		}
		store.put(key, getBytes(id));
	}

	public void invalidate(CategorizedDescriptor d) {
		var key = getKey(d);
		invalidate(key);
	}

	private void invalidate(String key) {
		var split = key.split("/");
		for (var i = 0; i < split.length; i++) {
			var k = "";
			for (var j = 0; j <= i; j++) {
				k += split[j];
				if (j < i) {
					k += "/";
				}
			}
			store.remove(k);
		}
	}

	private String getKey(CategorizedDescriptor d) {
		var path = categoryPath.path(d.category);
		if (d.type == ModelType.CATEGORY)
			return getKey(((CategoryDescriptor) d).categoryType, path, d.name);
		return getKey(d.type, path, d.refId + (asProto ? ".proto" : ".json"));
	}

	private String getKey(ModelType type, String path, String name) {
		var key = type.name();
		if (path != null && !path.isBlank()) {
			if (!path.startsWith("/")) {
				path = "/" + path;
			}
			key += path;
		}
		if (name != null && !name.isBlank()) {
			key += "/" + name;
		}
		return key;
	}

	private byte[] getBytes(ObjectId id) {
		var bytes = new byte[40];
		id.copyRawTo(bytes, 0);
		return bytes;
	}

}
