
require('dotenv').config();
let throng = require("throng");
let Queue = require("bull");
const admin = require("firebase-admin");
const csv = require("csv-stream");
const { logger } = require("./helpers/logger");
const { firestore } = require("firebase-admin");
const request = require("request");
const progress = require("request-progress");
const storeData = require('./utilities/write_to_db');
const validateJSONData = require('./utilities/clean_transformer');
const pickRandom = require('pick-random');
const openDatabase = require('./utilities/mongo_client');
const BatchStream = require('batch-stream');
const moment = require('moment');
// patch fix

const productionRedis = {
  redis: {
    port: 6379,
    host: "127.0.0.1",
    password:
      process.env.REDIS_PASSWORD_RAFFLE,
  },
};
const developmentRedis =  "redis://127.0.0.1:6379";

let workers = process.env.WEB_CONCURRENCY || 2;

let maxJobsPerWorker = 50;

const serviceAccount = require("./service-account.json");

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
});




function start() {
  let workQueue = new Queue("work", productionRedis);
  workQueue.process(maxJobsPerWorker, function(job, done){

    const {url, name, count, operation, weekDuration} = job.data;
    const generateLucky10 = job.data['generateLucky10'];
    const csvStream = csv.createStream();
    //var duplicateCount = {};
    if(operation === 'delete'){
      deleteColletions(name, count, job, done);
      return;
    }
  
    if(generateLucky10){
      logger.info('Generate lucky 10 *****');
      getLucky10(name, count, job, done);
      return;
    }


    openDatabase(`${name}_week_${count}_customer_details`,
    `${name}_week_${count}_migration`).then(client => {
      let totalDocsCount = 0;
      const batch = new BatchStream({size: 500});
      const operation = "DATA_CREATION";
      job.progress({current: 0, remaining: -1, operationType: operation, 
      count:count, name:name, docsCount: 0});
     // check for errors
     csvStream.on("error", (error) => {
       console.log("CSv stream error", error);
       throw error;
     });

     // count data coming in
      csvStream.on('data', (data) => {
        totalDocsCount += 1;
        job.progress({current: totalDocsCount, remaining:-1, operationType: operation, count:count, name:name, 
          docsCount: totalDocsCount});
      });

      progress(request(url))
      .pipe(csvStream)
      .pipe(validateJSONData())
      .pipe(batch).
       pipe(storeData(client.collection, client.migration))
      .on("finish", () => {
        openDatabase(`${name}_week_${count}_customer_details`,
         `${name}_week_${count}_migration`).then((newclient) => {
        const getSample =  newclient.collection.find().limit(300).toArray(); // client.collection.find();//limit(80000)
        getSample.then(results => {
          writePointsAndDetails(results.reverse(), name, count, job, done).then(() => {
          job.progress({current: 100, remaining:100, 
              operationType: operation, count:count, name:name, 
              docsCount: totalDocsCount});
            done();
            client.close();
          });
        }).catch(err => {
          console.log("Failed to query", err);
          const error = `An error occured while processing please try again`;
          job.progress(error);
        });
      
        }).catch(err => {
          const error = `Please check your csv file for missing 
          blank customer numbers and empty fields`;
          job.progress(error);
          logger.info("Failed to store adtition data to firebase")
        });
      }).on("error", (err) => {
        logger.info("An error occured while finishing: => ", err);
      });
    }).catch((err) => {
      logger.info("Failed to open db: => ",err);
    });
  });

  
}

async function writePointsAndDetails(datas, name, count, job, done){
 try{
  const operation = "DATA_CREATION";
  const documentSnapshotArray = datas;
  const total_count = datas.length;
  const batchArrayDetails = [];
  batchArrayDetails.push(firestore().batch());
  let operationCounter = 0;
  let batchIndex = 0;
  documentSnapshotArray.forEach((csv_doc) => {
    const uid = `${csv_doc['Customer Number']}`;
    const documentDataDetails = firestore().collection(`${name}_week_${count}_customer_details`).doc(uid);
    batchArrayDetails[batchIndex].set(documentDataDetails, {
      "Customer Number": csv_doc["Customer Number"],
      "Loan Reference": csv_doc["Loan Reference"],
      "Loan Repaid Date": moment(csv_doc["Loan Repaid Date"]).format(),
      "Loan Start Date": moment(csv_doc["Loan Start Date"]).format()
    });
    operationCounter++;
    
    job.progress({current: operationCounter, remaining: total_count, operationType: operation, 
       count:count, name:name, docsCount: total_count});

    if (operationCounter === 499) {
      batchArrayDetails.push(firestore().batch());
      batchIndex++;
      operationCounter = 0;
    }
  });

  batchArrayDetails.forEach(async (batch) => await batch.commit());
  job.progress({current: total_count, remaining:total_count, operationType: operation, count:count, name:name, 
    docsCount: total_count});
  done();
  return true;
 }catch(e){
   throw e;
 }
}


async function deleteColletions(name, count, job, done){
  const operation = "DELETION";
  const _dropMongoCollection = await deleteMongoCollection(name, count);
  const documentSnapshotArrayDetails = await firestore().collection(`${name}_week_${count}_customer_details`).listDocuments();
  const documentSnapshotArrayWinners =  await firestore().collection(`${name}_week_${count}_winners`).doc(`${count}`).delete();
  const resetToZero = await firestore().collection(`${name}_week_${count}_counter`).doc(`${count}`).set({current_count: 0}, {merge: true});

  const batchArrayDetails = [];
  batchArrayDetails.push(firestore().batch());
  let operationCounterDetails = 0;
  let batchIndexDetails = 0;
 
  const totalDetailsWinnersCount = documentSnapshotArrayDetails.length;
  


    documentSnapshotArrayDetails.forEach((csv_doc) => {
      batchArrayDetails[batchIndexDetails].delete(csv_doc);
      operationCounterDetails++;
      
      job.progress({current: operationCounterDetails, remaining:0,  operationType: operation, count:count, name:name, 
        docsCount: totalDetailsWinnersCount});
      if (operationCounterDetails === 499) {
        batchArrayDetails.push(firestore().batch());
        batchIndexDetails++;
        operationCounterDetails = 0;
      }
    });
    
  
    const enableRandomiseButton = batchArrayDetails.forEach(async (batch) => await batch.commit());

   const resetCount = await firestore().collection('all_projects')
    .doc(name).collection('week_state_draw')
    .doc(`week_${count}`).set({randomised: false}, {merge: true});
    job.progress({current: 100, remaining:100, operationType: operation, count:count, name:name, docsCount: totalDetailsWinnersCount});
    done();
    // job.discard()
    
}


async function deleteMongoCollection(name, count){
  try{
    const _collection = await openDatabase(`${name}_week_${count}_customer_details`,
    `${name}_week_${count}_migration`);
    const dropCollection = await _collection.collection.drop();
    const _dropCollectionMigrations = await _collection.migration.drop();
    return true;
  }catch(e){
    return false;
  }
}


async function getLucky10(name, count, job, done){
  try{
    // let details_index = 1;
    // let docsReferences = [];
    // let documentSnapshotArrayDetails = [];
    //let lucky_weekly_10_winners = [];
    const details_batch = firestore().batch();
    let progress = 0;
    // while(details_index <= count){
    //   docsReferences.push(firestore().collection(`${name}_week_${details_index}_customer_details`).limit(777).get());
    //   progress += 1;
    //   job.progress({current: progress, remaining: 0});
    //   details_index++;
    // }
   
  
    // await Promise.all(docsReferences).then((docs) => {
    //  docs.map(doc => doc.forEach(user_details => {
    //     documentSnapshotArrayDetails.push(user_details.data())
    //     progress += 1;
    //     job.progress({current: progress, remaining: 0});
    //   }));
      
    // });

    const checkWinnersAlready = await firestore().collection(`${name}_grand_total_details`).get();
      const checkerArray = [];
      checkWinnersAlready.forEach(customer => checkerArray.push(customer));
      if(checkerArray.length > 1){
        job.progress({current: 100, remaining: 100});
        done();
        return;
      }


    openDatabase(`${name}_week_${count}_customer_details`,
    `${name}_week_${count}_migration`).then(client => {
        const unionCollections = [];
        for(let start=count-1;start >= 1; start--){
            unionCollections.push(
                { "$unionWith": {
                    "coll": `${name}_week_${count}_customer_details`,
                    "pipeline": [{
                        "$project": { 
                            "Customer Number": true, 
                            "Loan Reference": true , 
                            "Loan Repaid Date": true, 
                            "Loan Start Date": true,
                            "_id": 0}
                    }]
                } }
        );
        }
        const pipeline = [
            { "$project": { 
                "Customer Number": true, 
                "Loan Reference": true , 
                "Loan Repaid Date": true, "Loan Start Date": true, "_id": 0} },
                ...unionCollections,
                {"$sample": {"size": 10}}
        ];
      
      client.collection.aggregate(pipeline).toArray().then(winners => {
      const lucky_weekly_10_winners = winners.map(function(lucky_winners){
        return {
          "Customer Number": lucky_winners["Customer Number"],
          "Loan Reference": lucky_winners["Loan Reference"],
          "Loan Repaid Date": lucky_winners["Loan Repaid Date"],
          "Loan Start Date": lucky_winners["Loan Start Date"]

        };
      });
      // check if winners already entred
      
      lucky_weekly_10_winners.forEach(csv_doc => {
      const uid = `${csv_doc['Customer Number']}`;
      let details = firestore().collection(`${name}_grand_total_details`).doc(uid);
      details_batch.set(details, csv_doc);
      progress++;
    });

    details_batch.commit();
    setFinalGrandCount(name, count, job, done);
       });
       
    })

    

  }catch(e){
    const message = `Picking lucky winners failed`;
    job.progress(message);
    done(new Error(message));
    logger.info(message, e);
  }
  
}

async function setFinalGrandCount(name, count, job, done){
  const participantsCollection = await firestore().collection(`${name}_total_week_count`).doc(name);
    const allparticipants = await firestore().collection(`${name}_total_week_count`).doc(name).get();
    if(allparticipants.exists){
      return;
    }
    openDatabase(`${name}_week_${count}_customer_details`,
    `${name}_week_${count}_migration`).then(client => {
        const unionCollections = [];
        for(let start=count-1;start >= 1; start--){
            unionCollections.push(
                { "$unionWith": {
                    "coll": `${name}_week_${count}_customer_details`,
                    "pipeline": [{
                        "$project": { 
                            "Customer Number": true, 
                            "Loan Reference": true , 
                            "Loan Repaid Date": true, 
                            "Loan Start Date": true,
                            "_id": 0}
                    }]
                } }
        );
        }
        const pipeline = [
            { "$project": { 
                "Customer Number": true, 
                "Loan Reference": true , 
                "Loan Repaid Date": true, "Loan Start Date": true, "_id": 0} },
                ...unionCollections,
                {"$count": "totalCount"}
        ];
      
       
       const report = client.collection.aggregate(pipeline, (err, res) => {
        const processCounter = new Promise((resolve, reject) => {
         if(err) reject(err);
         res.forEach(doc => {
           let migrationcount;
           migrationcount = doc['totalCount'];
           if(migrationcount){
             resolve(migrationcount);
           }
          });
       });

       processCounter.then(migrationcount => {
        participantsCollection.set({current_count: migrationcount}, {merge: true}).then(() => null);
        job.progress({current: 100, remaining: 100});
        done();
       });
        
      });
       
    })
}


// See: https://devcenter.heroku.com/articles/node-concurrency for more info
throng({ workers, start });
