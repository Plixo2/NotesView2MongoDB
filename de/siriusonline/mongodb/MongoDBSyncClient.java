package de.siriusonline.mongodb;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.Vector;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.DistinctIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.result.DeleteResult;

import lotus.domino.AgentBase;
import lotus.domino.AgentContext;
import lotus.domino.Database;
import lotus.domino.DateTime;
import lotus.domino.Item;
import lotus.domino.NotesException;
import lotus.domino.Session;
import lotus.domino.View;
import lotus.domino.ViewEntry;
import lotus.domino.ViewEntryCollection;

public class MongoDBSyncClient {
	private static final String NOTES_ID = "_notesUnid";
	public static boolean DEBUG = true;

	private String hostname;
	private int port;
	private String username;
	private String password;
	private String database;

	private Session session;
	
	public MongoDBSyncClient(Session session, String hostname, int port, String username, String password,
			String database) {
		this.hostname = hostname;
		this.port = port;
		this.username = username;
		this.password = password;
		this.database = database;
		this.session = session;
	}

	public void sync(ExportConfig config) {
		log("Started database sync");
		try {
			MongoCredential credential = MongoCredential.createCredential(username, database, password.toCharArray());

			MongoClientOptions options = MongoClientOptions.builder().sslEnabled(false).build();
			try (MongoClient mongoClient = new MongoClient(new ServerAddress(hostname, port), credential, options)) {
				log("created database connection");
				MongoDatabase database = mongoClient.getDatabase(this.database);

				syncNotesView(session, config, database);
				log("converting done");
			} catch (Exception e) {
				e.printStackTrace();
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void syncNotesView(Session session, ExportConfig config, MongoDatabase database) throws NotesException {
		Database db = null;
		View view = null;
		ViewEntryCollection vec = null;
		ViewEntry ve = null;
		try {
			db = session.getCurrentDatabase();
			view = db.getView(config.getViewName());

			vec = view.getAllEntries();
			ve = vec.getFirstEntry();

			boolean doesCollectionExists = false;
			for (String name : database.listCollectionNames()) {
				if (name.equals(config.getCollection())) {
					doesCollectionExists = true;
					break;
				}
			}
			if (!doesCollectionExists) {
				database.createCollection(config.getCollection());
			}
			final MongoCollection<Document> collection = database.getCollection(config.getCollection());
			final DistinctIterable<String> foundDatabase = collection.distinct(NOTES_ID, null, String.class);
			final Set<String> databaseSet = new HashSet<>();
			final Set<String> notesSet = new HashSet<>();

			for (String nID : foundDatabase) {
				databaseSet.add(nID);
			}

			log("found " + collection.countDocuments() + " documents");

			int replaced = 0;
			int skipped = 0;
			int added = 0;

			while (ve != null) {
				lotus.domino.Document notesDocument = null;
				Vector<Item> items = null;
				try {
					notesDocument = ve.getDocument();
					items = notesDocument.getItems();
					final Document mongoDocument = new Document();
					for (Item item : items) {
						String name = item.getName();
						if (config.getFields().isEmpty() || config.getFields().contains(name)) {
							final Object value = getDominoValueByItem(item);
							name = name.replaceAll("\\$", "");
							if (String.valueOf(value).trim().isEmpty()) {
								skipped += 1;
								continue;
							}
							mongoDocument.append(name, value);
						}
					}
					final String universalID = notesDocument.getUniversalID();
					mongoDocument.append(NOTES_ID, universalID);
					notesSet.add(universalID);
					if (databaseSet.contains(universalID)) {
						collection.replaceOne(Filters.eq(NOTES_ID, universalID), mongoDocument);
						replaced += 1;
					} else {
						collection.insertOne(mongoDocument);
						added += 1;
					}
				} finally {
					final ViewEntry nextEntry = vec.getNextEntry(ve);
					if (items != null) {
						for (Item item : items) {
							item.recycle();
						}
					}
					if (notesDocument != null) {
						notesDocument.recycle();
					}
					ve.recycle();
					ve = nextEntry;
				}
			}
			DeleteResult deleteMany = collection
					.deleteMany(Document.parse("{ \"" + NOTES_ID + "\": {\"$exists\": false }}"));

			log("found " + databaseSet.size() + " existing entries");
			databaseSet.removeAll(notesSet);
			databaseSet.forEach(ref -> {
				collection.deleteOne(Filters.eq(NOTES_ID, ref));
			});

			log("added " + added + " entries");
			log("skipped " + skipped + " keys");
			log("updated " + replaced);
			log("removed " + (databaseSet.size() + deleteMany.getDeletedCount()));

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (ve != null) {
				ve.recycle();
				ve = null;
			}
			if (vec != null) {
				vec.recycle();
				vec = null;
			}
			if (view != null) {
				view.recycle();
				view = null;
			}
			if (db != null) {
				db.recycle();
				db = null;
			}
		}
	}

	public Object getDominoValueByItem(Item item) throws NotesException {
		switch (item.getType()) {
		case Item.DATETIMES:
			return ((DateTime) item.getValueDateTimeArray().get(0)).toJavaDate();

		case Item.NUMBERS:
			return item.getValueDouble();

		case Item.HTML:
		case Item.USERID:
		case Item.RICHTEXT:
		case Item.NAMES:
		case Item.READERS:
		case Item.TEXT:
		case Item.AUTHORS:
		default:
			return item.getText();
		}
	}
	
	private void log(String str) {
		if(DEBUG)
		System.out.println("[MongoDBSyncClient]: " + str);
	}


}