
require('dotenv').config();
let throng = require("throng");
let Queue = require("bull");
const admin = require("firebase-admin");
const csv = require("csv-stream");
const { logger } = require("./helpers/logger");
const { firestore } = require("firebase-admin");
const request = require("request");
const progress = require("request-progress");
const {updateWeeklyState, setDocumentCount, deleteLucky3} = require('./helpers/parallel_add');
const pickRandom = require('pick-random');
const moment = require('moment');
// process.env.REDIS_URL || "redis://127.0.0.1:6379";
/*
{
    redis: {
      port: 6379,
      host: "127.0.0.1",
      password:
        process.env.REDIS_PASSWORD_RAFFLE,
    },
  }
*/
let workers = process.env.WEB_CONCURRENCY || 2;

let maxJobsPerWorker = 50;

const serviceAccount = require("./service-account.json");

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
});

function start() {
  let workQueue = new Queue("work", {
    redis: {
      port: 6379,
      host: "127.0.0.1",
      password:
        process.env.REDIS_PASSWORD_RAFFLE,
    },
  });

  workQueue.process(maxJobsPerWorker, async (job) => {
    try {
      const datas = [];
      const {url, name, count, operation, weekDuration} = job.data;
      const generateLucky10 = job.data['generateLucky10'];
      const csvStream = csv.createStream();
      var duplicateCount = {};

      if(operation === 'delete'){
        deleteColletions(name, count, job);
        return;
      }

      if(generateLucky10){
        console.log('Generate lucky 10 *****');
        getLucky10(name, count, job);
        return;
      }

      progress(request(url))
        .on("progress", function (state) {
          logger.info("progress", state);
        })
        .pipe(csvStream)
        .on("error", function (err) {
          logger.error(err);
          job.progress('Oops an internal error occured, please contact support');
        })
        .on("data", function (csv_data) {
          if(csv_data["Customer Number"].trim() && 
          csv_data["Loan Reference"].trim() && 
          csv_data["Loan Repaid Date"].trim() 
          && csv_data["Loan Start Date"].trim()){
            // check for duplicates
           
            const key = csv_data["Loan Reference"].trim();

            if(duplicateCount[key] === 0){
              duplicateCount[key]++;
                        }else{
                          duplicateCount[key] = 0;
                         
                        }
            
                      if(duplicateCount[key] >= 1){
                          const eror_message = `Please check your csv file for duplicates`;
                          job.progress(eror_message);
                          throw Error(eror_message);
                        }

                        datas.push({
                          "Customer Number": csv_data["Customer Number"].trim(),
                          "Loan Reference": csv_data["Loan Reference"].trim(),
                          "Loan Repaid Date": moment(csv_data["Loan Repaid Date"]).format(),
                          "Loan Start Date": moment(csv_data["Loan Start Date"]).format(),
                        });
  
          }else{
            const eror_message = `Please check your csv file for missing 
            blank customer numbers and empty fields`;
            job.progress(eror_message);
            throw Error(eror_message);
          }
        })
        .on("end", async function (data) {

          if(parseInt(count) > 1){
            const diff = count - 1;
            const customerDetails = await firestore().collection(`${name}_week_${diff}_customer_details`).get();
            customerDetails.forEach(customer_details => datas.push(customer_details.data()));
          }
 

          if(parseInt(count) === parseInt(weekDuration)){
            writePointsAndDetails(datas, name, count, job);
            return;
          }
          
          writePointsAndDetails(datas, name, count, job);
        });
    } catch (e) {
      logger.info("WORKER ERROR", e);
    }
  });
}

async function writePointsAndDetails(datas, name, count, job){
  const operation = "DATA_CREATION";
  const documentSnapshotArray = datas;
  const total_count = datas.length;
  const batchArrayDetails = [];
  batchArrayDetails.push(firestore().batch());
  let operationCounter = 0;
  let batchIndex = 0;
  documentSnapshotArray.forEach((csv_doc) => {
    const uid = `${csv_doc['Loan Reference']}`.trim();
    const documentDataDetails = firestore().collection(`${name}_week_${count}_customer_details`).doc(uid);
    batchArrayDetails[batchIndex].set(documentDataDetails, {...csv_doc});
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
}


async function deleteColletions(name, count, job){
  const operation = "DELETION";
  let documentSnapshotArrayDetails = await firestore().collection(`${name}_week_${count}_customer_details`).listDocuments();
  let documentSnapshotArrayWinners =  await firestore().collection(`${name}_week_${count}_winners`).listDocuments();
  const batchArrayDetails = [];
  const batchWinners = [];
  batchArrayDetails.push(firestore().batch());
  batchWinners.push(firestore().batch());
  let operationCounterDetails = 0;
  let batchIndexDetails = 0;
 
  let operationCounterWinners = 0;
  let batchIndexWinners = 0;
  const totalDetailsWinnersCount = documentSnapshotArrayDetails.length + documentSnapshotArrayWinners.length;
  


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

   
    if(documentSnapshotArrayWinners.length >= 1){
      let counter_progress = operationCounterDetails;
      documentSnapshotArrayWinners.forEach((csv_doc) => {
        batchWinners[batchIndexWinners].delete(csv_doc);
        operationCounterWinners++;
        counter_progress += operationCounterWinners;
        job.progress({current: counter_progress, remaining: 0, operationType: operation, 
          count:count, name:name, docsCount: totalDetailsWinnersCount});
      });
      batchWinners.forEach(async (batch) => await batch.commit());
    }
    
  
    batchArrayDetails.forEach(async (batch) => await batch.commit());
    job.progress({current: 100, remaining:100, operationType: operation, count:count, name:name, docsCount: totalDetailsWinnersCount});
  
}


async function getLucky10(name, count, job){
  try{
    let details_index = 1;
    let docsReferences = [];
    let documentSnapshotArrayDetails = [];
    let lucky_weekly_10_winners = [];
    const details_batch = firestore().batch();
    let progress = 0;
    
    
    while(details_index <= count){
      docsReferences.push(firestore().collection(`${name}_week_${details_index}_customer_details`).get());
      progress += 1;
      job.progress({current: progress, remaining: 0});
      details_index++;
    }
   
  
    await Promise.all(docsReferences).then((docs) => {
     docs.map(doc => doc.forEach(user_details => {
        documentSnapshotArrayDetails.push(user_details.data())
        progress += 1;
        job.progress({current: progress, remaining: 0});
      }));
      
    });


      lucky_weekly_10_winners = pickRandom(documentSnapshotArrayDetails, {count: 10});
      progress++;
      job.progress({current: progress, remaining: 0});
      lucky_weekly_10_winners.forEach(csv_doc => {
      const uid = `${csv_doc['Loan Reference']}`.trim();
      let details = firestore().collection(`${name}_grand_total_details`).doc(uid);
      details_batch.set(details, csv_doc);
      progress++;
      job.progress({current: progress, remaining: 0});
    });
    details_batch.commit();
    job.progress({current: 100, remaining: 0});
  }catch(e){
    job.progress(`Picking lucky winners failed`);
    logger.info('Error while picking lucky 10', e);
  }
  
}


// See: https://devcenter.heroku.com/articles/node-concurrency for more info
throng({ workers, start });
