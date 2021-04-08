package org.openlca.git;

import org.eclipse.jgit.internal.storage.file.FileRepository;
import org.eclipse.jgit.lib.PersonIdent;
import org.openlca.core.database.IDatabase;

public class Config {

	public final IDatabase database;
	public final FileRepository repo;
	public final PersonIdent committer;
	public final boolean asProto;
	public boolean checkExisting = true;
	public int converterThreads = 8;

	private Config(IDatabase database, FileRepository repo, PersonIdent committer, boolean asProto) {
		this.database = database;
		this.repo = repo;
		this.committer = committer;
		this.asProto = asProto;
	}

	public static Config newJsonConfig(IDatabase database, FileRepository repo, PersonIdent committer) {
		return new Config(database, repo, committer, false);
	}

	public static Config newProtoConfig(IDatabase database, FileRepository repo, PersonIdent committer) {
		return new Config(database, repo, committer, true);
	}

}