const MongoClient = require('mongodb').MongoClient;
const mongodb_url = 'mongodb://localhost:27017';
const databaseName = 'ncba';
const schema = {
  validator: {
    "$jsonSchema": {
      bsonType: "object",
      required: [
      "Customer Number", 
      "Loan Reference", 
      "Loan Repaid Date",
      "Loan Start Date"
    ]
    },
    properties: {
      "Customer Number": {
        bsonType: "string",
        description: "Customer number must be a string",
        pattern: "^[A-Za-z]+$"
      },
      "Loan Reference": {
        bsonType: "string",
        description: "Loan Reference must be a string",
        pattern: "^[A-Za-z]+$"
      },
      "Loan Repaid Date": {
        bsonType: "string",
        description: "Loan Repaid Date must be a string",
        pattern: "^[A-Za-z]+$"
      },
      "Loan Start Date": {
        bsonType: "string",
        description: "Loan Start Date must be a string",
        pattern: "^[A-Za-z]+$"
      }
    }
  }
};


function openDatabase(collectionName, migrationCollectionName=""){
    return  MongoClient.connect(mongodb_url).then(client => {
      const db = client.db(databaseName);
      const collection = db.collection(collectionName)
      const migration = db.collection(migrationCollectionName);
      return {
        migration,
        collection,
        close: () => {
          return client.close();
        }
      };
    });
  }


module.exports = openDatabase;