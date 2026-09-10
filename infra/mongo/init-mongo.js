'use strict';

(() => {
    const databaseName = process.env.MONGO_DATABASE;
    const username = process.env.MONGO_APP_USERNAME;
    const password = process.env.MONGO_APP_PASSWORD;

    if (!databaseName || !username || !password) {
      throw new Error("MONGO_DATABASE, MONGO_APP_USERNAME and MONGO_APP_PASSWORD are required");
    }

    const appDb = db.getSiblingDB(databaseName);

    if (!appDb.getUser(username)) {
      appDb.createUser({
        user: username,
        pwd: password,
        roles: [{ role: "readWrite", db: databaseName }]
      });
      print(`Created MongoDB app user '${username}' for '${databaseName}'.`);
    } else {
      print(`MongoDB app user '${username}' already exists.`);
    }
})();