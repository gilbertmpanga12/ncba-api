
require('dotenv').config();
let throng = require("throng");
let Queue = require("bull");
const admin = require("firebase-admin");
const csv = require("csv-stream");
const { logger } = require("./helpers/logger");
// const { nanoid } = require("nanoid");
// const { firestore } = require("firebase-admin");
const request = require("request");
const progress = require("request-progress");
const {writePointsAndDetails, deleteColletions} = require('./helpers/parallel_add');

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



// See: https://devcenter.heroku.com/articles/node-concurrency for more info
throng({ workers, start });
