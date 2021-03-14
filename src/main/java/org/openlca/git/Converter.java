package org.openlca.git;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.openlca.core.database.IDatabase;
import org.openlca.core.model.descriptors.Descriptor;
import org.openlca.util.Pair;
import org.slf4j.LoggerFactory;

class Converter implements Runnable {

  private final Config config;
  private final Descriptor descriptor;

  Converter(Config config, Descriptor descriptor) {
    this.config = config;
    this.descriptor = descriptor;
  }

  @Override
  public void run() {
    try {
      var model = config.db.get(
        descriptor.type.getModelClass(), descriptor.id);
      var data = config.asProto
        ? ProtoWriter.toProto(model, config.db)
        : ProtoWriter.toJson(model, config.db);
      addNext(data == null
        ? config.EMPTY
        : Pair.of(descriptor, data));
    } catch (Exception e) {
      var log = LoggerFactory.getLogger(getClass());
      log.error("failed to convert data set " + descriptor, e);
      addNext(config.EMPTY);
    }
  }

  private void addNext(Pair<Descriptor, byte[]> pair) {
    try {
      while (!config.queue.offer(pair, 5, TimeUnit.SECONDS)) {
        var log = LoggerFactory.getLogger(getClass());
        log.warn("data queue blocked; waiting");
      }
    } catch (InterruptedException e) {
      var log = LoggerFactory.getLogger(getClass());
      log.error("failed to add element to data queue", e);
    }
  }

  static Config newJsonConfig(IDatabase db) {
    return new Config(db, false);
  }

  static Config newProtoConfig(IDatabase db) {
    return new Config(db, true);
  }

  static class Config {

    final Pair<Descriptor, byte[]> EMPTY = Pair.of(null, null);

    private final IDatabase db;
    final boolean asProto;
    final ArrayBlockingQueue<Pair<Descriptor, byte[]>> queue;

    private Config(IDatabase db, boolean asProto) {
      this.db = db;
      this.asProto = asProto;
      this.queue = new ArrayBlockingQueue<>(50);
    }
  }
}
