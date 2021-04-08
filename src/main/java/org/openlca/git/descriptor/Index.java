package org.openlca.git.descriptor;

import java.util.ArrayList;
import java.util.List;

import org.openlca.core.model.descriptors.CategorizedDescriptor;
import org.openlca.core.model.descriptors.Descriptor;

import gnu.trove.map.hash.TLongObjectHashMap;

class Index extends TLongObjectHashMap<List<Descriptor>> {

	Index(List<? extends Descriptor> descriptors) {
		if (descriptors.isEmpty())
			return;
		init(descriptors);
	}

	private void init(List<? extends Descriptor> descriptors) {
		for (var d : descriptors) {
			if (!(d instanceof CategorizedDescriptor))
				continue;
			var descriptor = (CategorizedDescriptor) d;
			var categoryId = descriptor.category == null ? 0 : descriptor.category;
			var list = get(categoryId);
			if (list == null) {
				list = new ArrayList<>();
				put(categoryId, list);
			}
			list.add(descriptor);
		}

	}

}