let throng = require("throng");
let Queue = require("bull");
const admin = require("firebase-admin");
// const pickRandom = require('pick-random');
const csv = require("csv-stream");
const { logger } = require("./helpers/logger");
const { nanoid } = require("nanoid");
const { firestore } = require("firebase-admin");
const request = require("request");
const progress = require("request-progress");
const options = {
  columns: [
    "Customer Number",
    "Loan Reference",
    "Loan Start Date",
    "Loan Repaid Date",
  ],
};
// Connect to a local redis instance locally, and the Heroku-provided URL in production
let REDIS_URL =
  process.env.HEROKU_REDIS_GOLD_URL ||
  "redis://BpJqTatVLvgyUbTVT7Jv4BZLDyX6gaTERTuhlkTBXg3EV8MWjRk5uZI5EzRzR5OoW37lb+ONV8Ev9GOW@127.0.0.1:6379";

// Spin up multiple processes to handle jobs to take advantage of more CPU cores
// See: https://devcenter.heroku.com/articles/node-concurrency for more info
let workers = process.env.WEB_CONCURRENCY || 2;

// The maximum number of jobs each worker should process at once. This will need
// to be tuned for your application. If each job is mostly waiting on network
// responses it can be much higher. If each job is CPU-intensive, it might need
// to be much lower.
// to be refactored
let maxJobsPerWorker = 50;

const serviceAccount = require("./service-account.json");

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
});

function start() {
  // Connect to the named work queue
  // {redis: {port: 6379, host: '127.0.0.1', password: 'BpJqTatVLvgyUbTVT7Jv4BZLDyX6gaTERTuhlkTBXg3EV8MWjRk5uZI5EzRzR5OoW37lb+ONV8Ev9GOW'}}

  let workQueue = new Queue("work", {
    redis: {
      port: 6379,
      host: "127.0.0.1",
      password:
        "BpJqTatVLvgyUbTVT7Jv4BZLDyX6gaTERTuhlkTBXg3EV8MWjRk5uZI5EzRzR5OoW37lb+ONV8Ev9GOW",
    },
  });

  workQueue.process(maxJobsPerWorker, async (job) => {
    try {
      // const {url, name, count} = job.data;
      const url =
        "https://firebasestorage.googleapis.com/v0/b/wholesaleduuka-418f1.appspot.com/o/NCBA%2F1615411998226_Test%20file%208.csv?alt=media&token=bb9cac79-b72c-4fbf-9d1f-005491c79f53";
      const name = "Holla";
      const count = 8;
      let datas = [];
      // const name = job.data['name'];
      // const count = job.data['count'];
      // const datas = job.data['user_details'];
      const csvStream = csv.createStream(options);
      progress(request(url))
        .on("progress", function (state) {
          console.log("progress", state);
        })
        .pipe(csvStream)
        .on("error", function (err) {
          console.error(err);
        })
        .on("data", function (csv_data) {
          datas.push({
            "Customer Number": csv_data["Customer Number"],
            "Loan Reference": csv_data["Loan Reference"],
            "Loan Repaid Date": csv_data["Loan Repaid Date"],
            "Loan Start Date": csv_data["Loan Start Date"],
          });
        })
        .on("end", async function (data) {
          const documentSnapshotArray = datas;
          const total_count = datas.length;
          const batchArrayPoints = [];
          const batchArrayDetails = [];
          batchArrayPoints.push(firestore().batch());
          batchArrayDetails.push(firestore().batch());
          let operationCounter = 0;
          let batchIndex = 0;
          documentSnapshotArray.forEach((csv_doc) => {
            const uid = nanoid(10);
            const documentDataPoints = firestore().collection(`${name}_week_${count}_customer_points`).doc(uid);
            const documentDataDetails = firestore().collection(`${name}_week_${count}_customer_details`).doc(uid);
            console.log(csv_doc);
            batchArrayPoints[batchIndex].set(documentDataPoints, {customerId: csv_doc['Customer Number'],
            loanReference: csv_doc['Loan Reference'], uid});
            batchArrayDetails[batchIndex].set(documentDataDetails, {...csv_doc, uid});
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
          job.progress(`${total_count}/${total_count}`); // done
        });
    } catch (e) {
      logger.info("WORKER ERROR", e);
    }
  });
}

// minor changes
// Initialize the clustered worker process
// See: https://devcenter.heroku.com/articles/node-concurrency for more info
throng({ workers, start });
