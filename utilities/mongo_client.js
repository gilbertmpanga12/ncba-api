const MongoClient = require('mongodb').MongoClient;
const mongodb_url = 'mongodb://localhost:27017';
const databaseName = 'ncba';


function openDatabase(collectionName){
    return  MongoClient.connect(mongodb_url).then(client => {
      const db = client.db(databaseName);
      const collection = db.collection(collectionName);
      return {
        collection,
        close: () => {
          return client.close();
        }
      };
    });
  }


module.exports = openDatabase;