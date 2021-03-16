
require('dotenv').config();
let throng = require("throng");
let Queue = require("bull");
const admin = require("firebase-admin");
const csv = require("csv-stream");
const { logger } = require("./helpers/logger");
const { firestore } = require("firebase-admin");
const request = require("request");
const progress = require("request-progress");
const {updateWeeklyState} = require('./helpers/parallel_add');


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
      const {url, name, count, operation} = job.data;
      const csvStream = csv.createStream();
      if(operation === 'delete'){
        deleteColletions(name, count, job);
        return;
      }
      progress(request(url))
        .on("progress", function (state) {
          logger.log("progress", state);
        })
        .pipe(csvStream)
        .on("error", function (err) {
          logger.error(err);
          job.progress('Oops an internal error occured, please contact support');
        })
        .on("data", function (csv_data) {
          if(csv_data["Customer Number"] && 
          csv_data["Loan Reference"] && 
          csv_data["Loan Repaid Date"] 
          && csv_data["Loan Start Date"]){ // omit empty customer _ids
            datas.push({
              "Customer Number": csv_data["Customer Number"],
              "Loan Reference": csv_data["Loan Reference"],
              "Loan Repaid Date": csv_data["Loan Repaid Date"],
              "Loan Start Date": csv_data["Loan Start Date"],
            });
          }else{
            const eror_message = `Please check your csv file for missing 
            blank customer numbers, or empty fields and 
            also ensure column names are correctly named`;
            job.progress(eror_message);
            throw Error(eror_message);
          }
        })
        .on("end", async function (data) {
          writePointsAndDetails(datas, name, count, job);
        });
    } catch (e) {
      logger.info("WORKER ERROR", e);
    }
  });
}

async function writePointsAndDetails(datas, name, count, job){
  const documentSnapshotArray = datas;
  const total_count = datas.length;
  const batchArrayPoints = [];
  const batchArrayDetails = [];
  batchArrayPoints.push(firestore().batch());
  batchArrayDetails.push(firestore().batch());
  let operationCounter = 0;
  let batchIndex = 0;
  documentSnapshotArray.forEach((csv_doc) => {
    const uid = `${csv_doc['Customer Number']}${csv_doc['Loan Reference']}`;
    const documentDataPoints = firestore().collection(`${name}_week_${count}_customer_points`).doc(uid);
    const documentDataDetails = firestore().collection(`${name}_week_${count}_customer_details`).doc(uid);
    batchArrayPoints[batchIndex].set(documentDataPoints, {customerId: csv_doc['Customer Number'],
    loanReference: csv_doc['Loan Reference']});
    batchArrayDetails[batchIndex].set(documentDataDetails, {...csv_doc});
    operationCounter++;
    job.progress(`${operationCounter}/${total_count}`);

    if (operationCounter === 499) {
      batchArrayPoints.push(firestore().batch());
      batchArrayDetails.push(firestore().batch());
      batchIndex++;
      operationCounter = 0;
    }
  });

  batchArrayPoints.forEach(async (batch) => await batch.commit());
  batchArrayDetails.forEach(async (batch) => await batch.commit());
  job.progress(`${total_count}/${total_count}`);
}


async function deleteColletions(name, count, job){
  let documentSnapshotArrayPoints = await firestore().collection(`${name}_week_${count}_customer_points`).listDocuments();
  let documentSnapshotArrayDetails = await firestore().collection(`${name}_week_${count}_customer_details`).listDocuments();
  const batchArrayPoints = [];
  const batchArrayDetails = [];
  batchArrayPoints.push(firestore().batch());
  batchArrayDetails.push(firestore().batch());
  let operationCounter = 0;
  let batchIndex = 0;

  let operationCounterDetails = 0;
  let batchIndexDetails = 0;
  let final_progress = "";
  let total_count_details = documentSnapshotArrayDetails.length;
  let total_count_points = documentSnapshotArrayPoints.length;

    documentSnapshotArrayPoints.forEach((csv_doc) => {
      batchArrayPoints[batchIndex].delete(csv_doc);
      operationCounter++;
      final_progress += `X${operationCounter}/${total_count_points}`;
      job.progress(final_progress);
      if (operationCounter === 499) {
        batchArrayPoints.push(firestore().batch());
        batchIndex++;
        operationCounter = 0;
      }
    });

    documentSnapshotArrayDetails.forEach((csv_doc) => {
      batchArrayDetails[batchIndexDetails].delete(csv_doc);
      operationCounterDetails++;
      final_progress += `X${operationCounterDetails}/${total_count_details}`;
      job.progress(final_progress);
      if (operationCounterDetails === 499) {
        batchArrayDetails.push(firestore().batch());
        batchIndexDetails++;
        operationCounterDetails = 0;
      }
    });
  
    batchArrayPoints.forEach(async (batch) => await batch.commit());
    batchArrayDetails.forEach(async (batch) => await batch.commit());
    updateWeeklyState(count, name);
    job.progress(`${total_count_details}/${total_count_points}`);
  
}


// See: https://devcenter.heroku.com/articles/node-concurrency for more info
throng({ workers, start });
