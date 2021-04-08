package org.openlca.git.repo;

import java.nio.charset.StandardCharsets;

import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import org.openlca.core.database.IDatabase;
import org.openlca.core.model.Actor;
import org.openlca.core.model.Currency;
import org.openlca.core.model.DQSystem;
import org.openlca.core.model.Flow;
import org.openlca.core.model.FlowProperty;
import org.openlca.core.model.ImpactCategory;
import org.openlca.core.model.ImpactMethod;
import org.openlca.core.model.Location;
import org.openlca.core.model.Parameter;
import org.openlca.core.model.Process;
import org.openlca.core.model.ProductSystem;
import org.openlca.core.model.Project;
import org.openlca.core.model.RootEntity;
import org.openlca.core.model.SocialIndicator;
import org.openlca.core.model.Source;
import org.openlca.core.model.UnitGroup;
import org.openlca.git.Config;
import org.openlca.proto.output.ActorWriter;
import org.openlca.proto.output.CurrencyWriter;
import org.openlca.proto.output.DQSystemWriter;
import org.openlca.proto.output.FlowPropertyWriter;
import org.openlca.proto.output.FlowWriter;
import org.openlca.proto.output.ImpactCategoryWriter;
import org.openlca.proto.output.ImpactMethodWriter;
import org.openlca.proto.output.LocationWriter;
import org.openlca.proto.output.ParameterWriter;
import org.openlca.proto.output.ProcessWriter;
import org.openlca.proto.output.ProductSystemWriter;
import org.openlca.proto.output.ProjectWriter;
import org.openlca.proto.output.SocialIndicatorWriter;
import org.openlca.proto.output.SourceWriter;
import org.openlca.proto.output.UnitGroupWriter;
import org.openlca.proto.output.WriterConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class ProtoWriter {

	private static final Logger log = LoggerFactory.getLogger(ProtoWriter.class);

	private ProtoWriter() {
	}

	static byte[] convert(RootEntity entity, Config config) {
		return config.asProto ? toProto(entity, config) : toJson(entity, config);
	}

	private static byte[] toProto(RootEntity entity, Config config) {
		if (entity == null)
			return null;
		var message = toMessage(entity, config.database);
		return message == null ? null : message.toByteArray();
	}

	private static byte[] toJson(RootEntity entity, Config config) {
		if (entity == null)
			return null;
		try {
			var message = toMessage(entity, config.database);
			if (message == null)
				return null;
			var json = JsonFormat.printer().print(message);
			return json.getBytes(StandardCharsets.UTF_8);
		} catch (Exception e) {
			log.error("failed to serialize " + entity, e);
			return null;
		}
	}

	private static Message toMessage(RootEntity entity, IDatabase db) {
		var conf = WriterConfig.of(db);

		if (entity instanceof Actor)
			return new ActorWriter(conf).write((Actor) entity);

		if (entity instanceof Currency)
			return new CurrencyWriter(conf).write((Currency) entity);

		if (entity instanceof DQSystem)
			return new DQSystemWriter(conf).write((DQSystem) entity);

		if (entity instanceof Flow)
			return new FlowWriter(conf).write((Flow) entity);

		if (entity instanceof FlowProperty)
			return new FlowPropertyWriter(conf).write((FlowProperty) entity);

		if (entity instanceof ImpactCategory)
			return new ImpactCategoryWriter(conf).write((ImpactCategory) entity);

		if (entity instanceof ImpactMethod)
			return new ImpactMethodWriter(conf).write((ImpactMethod) entity);

		if (entity instanceof Location)
			return new LocationWriter(conf).write((Location) entity);

		if (entity instanceof Parameter)
			return new ParameterWriter(conf).write((Parameter) entity);

		if (entity instanceof Process)
			return new ProcessWriter(conf).write((Process) entity);

		if (entity instanceof ProductSystem)
			return new ProductSystemWriter(conf).write((ProductSystem) entity);

		if (entity instanceof Project)
			return new ProjectWriter(conf).write((Project) entity);

		if (entity instanceof SocialIndicator)
			return new SocialIndicatorWriter(conf).write((SocialIndicator) entity);

		if (entity instanceof Source)
			return new SourceWriter(conf).write((Source) entity);

		if (entity instanceof UnitGroup)
			return new UnitGroupWriter(conf).write((UnitGroup) entity);

		return null;
	}

}
