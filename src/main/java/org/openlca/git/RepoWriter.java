package org.openlca.git;

import java.io.File;
import java.nio.charset.StandardCharsets;

import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;

import org.eclipse.jgit.internal.storage.file.FileRepository;
import org.eclipse.jgit.lib.CommitBuilder;
import org.eclipse.jgit.lib.Constants;
import org.eclipse.jgit.lib.FileMode;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.ObjectInserter;
import org.eclipse.jgit.lib.PersonIdent;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.lib.TreeFormatter;
import org.openlca.core.database.Derby;
import org.openlca.core.database.IDatabase;
import org.openlca.core.model.Actor;
import org.openlca.core.model.Currency;
import org.openlca.core.model.DQSystem;
import org.openlca.core.model.Flow;
import org.openlca.core.model.FlowProperty;
import org.openlca.core.model.ImpactCategory;
import org.openlca.core.model.ImpactMethod;
import org.openlca.core.model.Location;
import org.openlca.core.model.ModelType;
import org.openlca.core.model.Parameter;
import org.openlca.core.model.Process;
import org.openlca.core.model.ProductSystem;
import org.openlca.core.model.Project;
import org.openlca.core.model.SocialIndicator;
import org.openlca.core.model.Source;
import org.openlca.core.model.UnitGroup;
import org.openlca.core.model.Version;
import org.openlca.core.model.descriptors.Descriptor;
import org.openlca.git.DbTree.Node;
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
import org.slf4j.LoggerFactory;

public class RepoWriter {

  private final IDatabase db;
  private final Repository repo;
  private final PersonIdent committer;

  public RepoWriter(
    IDatabase db,
    Repository repo,
    PersonIdent committer) {
    this.repo = repo;
    this.db = db;
    this.committer = committer;
  }

  public void sync() {
    var start = System.currentTimeMillis();
    var dbTree = DbTree.build(db);
    var time = System.currentTimeMillis() - start;
    System.out.printf("loaded DB tree in %.3f sec%n", time / 1000d);

    try (var inserter = repo.newObjectInserter()) {

      // build the tree
      var root = new TreeFormatter();
      for (var type : ModelType.values()) {
        var node = dbTree.getRoot(type);
        if (node == null)
          continue;
        var id = syncTree(inserter, node);
        if (id != null) {
          root.append(type.name(), FileMode.TREE, id);
        }
      }

      // create the commit
      var treeID = root.insertTo(inserter);
      var commit = new CommitBuilder();
      commit.setAuthor(committer);
      commit.setCommitter(committer);
      commit.setMessage("first commit");
      commit.setEncoding(StandardCharsets.UTF_8);
      commit.setTreeId(treeID);
      var commitID = inserter.insert(commit);

      // update the head
      var ref = repo.findRef("HEAD");
      var update = repo.updateRef(ref.getName());
      update.setNewObjectId(commitID);
      update.update();

    } catch (Exception e) {
      var log = LoggerFactory.getLogger(getClass());
      log.error("failed to sync tree", e);
    }
  }

  private ObjectId syncTree(ObjectInserter inserter, Node node) {
    if (node.content.isEmpty() && node.childs.isEmpty())
      return null;
    var tree = new TreeFormatter();
    for (var child : node.childs) {
      var childID = syncTree(inserter, child);
      if (childID != null) {
        tree.append(child.name, FileMode.TREE, childID);
      }
    }
    for (var d : node.content) {
      if (d.type == null || d.type.getModelClass() == null)
        continue;
      var data = serialize(d);
      if (data == null)
        continue;
      try {
        var blobID = inserter.insert(Constants.OBJ_BLOB, data);
        var name = d.refId + "_" + Version.asString(d.version);
        tree.append(name, FileMode.REGULAR_FILE, blobID);
      } catch (Exception e) {
        var log = LoggerFactory.getLogger(getClass());
        log.error("failed to insert blob for " + d, e);
      }
    }
    try {
      return tree.insertTo(inserter);
    } catch (Exception e) {
      var log = LoggerFactory.getLogger(getClass());
      log.error("failed to insert tree", e);
      return null;
    }
  }

  private byte[] serialize(Descriptor d) {
    var model = db.get(d.type.getModelClass(), d.id);
    if (model == null)
      return null;

    Message message = null;
    var config = WriterConfig.of(db);

    if (model instanceof Actor) {
      var w = new ActorWriter(WriterConfig.of(db));
      message = w.write((Actor) model);
    } else if (model instanceof Currency) {
      var w = new CurrencyWriter(config);
      message = w.write((Currency) model);
    } else if (model instanceof DQSystem) {
      var w = new DQSystemWriter(config);
      message = w.write((DQSystem) model);
    } else if (model instanceof Flow) {
      var w = new FlowWriter(config);
      message = w.write((Flow) model);
    } else if (model instanceof FlowProperty) {
      var w = new FlowPropertyWriter(config);
      message = w.write((FlowProperty) model);
    } else if (model instanceof ImpactCategory) {
      var w = new ImpactCategoryWriter(config);
      message = w.write((ImpactCategory) model);
    } else if (model instanceof ImpactMethod) {
      var w = new ImpactMethodWriter(config);
      message = w.write((ImpactMethod) model);
    } else if (model instanceof Location) {
      var w = new LocationWriter(config);
      message = w.write((Location) model);
    } else if (model instanceof Parameter) {
      var w = new ParameterWriter(config);
      message = w.write((Parameter) model);
    } else if (model instanceof Process) {
      var w = new ProcessWriter(config);
      message = w.write((Process) model);
    } else if (model instanceof ProductSystem) {
      var w = new ProductSystemWriter(config);
      message = w.write((ProductSystem) model);
    } else if (model instanceof Project) {
      var w = new ProjectWriter(config);
      message = w.write((Project) model);
    } else if (model instanceof SocialIndicator) {
      var w = new SocialIndicatorWriter(config);
      message = w.write((SocialIndicator) model);
    } else if (model instanceof Source) {
      var w = new SourceWriter(config);
      message = w.write((Source) model);
    } else if (model instanceof UnitGroup) {
      var w = new UnitGroupWriter(config);
      message = w.write((UnitGroup) model);
    }

    if (message == null)
      return null;
    try {
      var json = JsonFormat.printer().print(message);
      return json.getBytes(StandardCharsets.UTF_8);
    } catch (Exception e) {
      var log = LoggerFactory.getLogger(getClass());
      log.error("failed to serialize " + d, e);
      return null;
    }
  }

  public static void main(String[] args) {

    var committer = new PersonIdent("msrocka", "test@some.mail.com");
    var repoDir = new File("target/testrepo/.git");

    try (var db = Derby.fromDataDir("ei22");
         var repo = new FileRepository(repoDir)) {
      if (!repoDir.exists()) {
        repo.create(true); // bare repo
        //repo.create();
      }
      new RepoWriter(db, repo, committer).sync();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

}
