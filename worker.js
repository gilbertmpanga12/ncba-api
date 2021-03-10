let throng = require('throng');
let Queue = require("bull");
const admin = require('firebase-admin');
// const pickRandom = require('pick-random');
const {logger} = require('./helpers/logger');
const { nanoid } = require('nanoid');
const { firestore } = require('firebase-admin');
// Connect to a local redis instance locally, and the Heroku-provided URL in production
let REDIS_URL = process.env.HEROKU_REDIS_GOLD_URL || "redis://BpJqTatVLvgyUbTVT7Jv4BZLDyX6gaTERTuhlkTBXg3EV8MWjRk5uZI5EzRzR5OoW37lb+ONV8Ev9GOW@127.0.0.1:6379";

// Spin up multiple processes to handle jobs to take advantage of more CPU cores
// See: https://devcenter.heroku.com/articles/node-concurrency for more info
let workers = process.env.WEB_CONCURRENCY || 2;

// The maximum number of jobs each worker should process at once. This will need
// to be tuned for your application. If each job is mostly waiting on network 
// responses it can be much higher. If each job is CPU-intensive, it might need
// to be much lower.
// to be refactored
let maxJobsPerWorker = 50;

const serviceAccount = require('./service-account.json');

admin.initializeApp({
    credential: admin.credential.cert(serviceAccount),
});

function start() {
  // Connect to the named work queue
  let workQueue = new Queue('work', REDIS_URL);

  workQueue.process(maxJobsPerWorker, async (job) => {
    try{
        let progress_details = 0;
        let progress_points = 0;
        const name = job.data['name'];
        const count = job.data['count'];
        const datas = job.data['user_details'];
        // USER POINTS
        const user_points = datas;
        let user_points_length = user_points.length;
        let counter_500s = 0;
        let customerPoints = firestore().batch();
        let remainder = user_points_length;
        for(var start=0; start <= user_points_length; start++){
            counter_500s += 1;
            if(remainder < 500){
                const uid = nanoid(10);
                const collection = firestore().collection(`${name}_week_${count}_customer_points`).doc(uid);
                customerPoints.set(collection, {customerId: user_points[start]['Customer Number'], 
                loanReference: user_points[start]['Loan Reference'], uid});
                await customerPoints.commit();
                // final increments
                progress_points = 50;
                job.progress(progress_points);
                break;
            }

            const uid = nanoid(10);
            const collection = firestore().collection(`${name}_week_${count}_customer_points`).doc(uid);
            customerPoints.set(collection, {customerId: user_points[start]['Customer Number'], 
            loanReference: user_points[start]['Loan Reference'], uid});
            if(counter_500s === 500){
                await customerPoints.commit();
                customerPoints = firestore().batch();
                counter_500s = 0;
                remainder -= 500;
                // minor increments
                progress_points += 1;
                job.progress(progress_points);
                continue;
            }
        }

        // USER DETAILS
        const user_details = datas;
        let user_details_length = datas.length;
        let counter_points_500s = 0;
        var customerDetails = firestore().batch();
        let remainder_details = user_details_length;
        for(var start=0; start <= user_details_length; start++){
            counter_points_500s += 1;
            if(remainder_details < 500){
                const uid = nanoid(10);
                const collection = firestore().collection(`${name}_week_${count}_customer_details`).doc(uid);
                customerDetails.set(collection, {...user_details[start], uid});
                await customerDetails.commit();// check for last
                // final increments
                progress_details = 50;
                job.progress(progress_details);
                break;
            }
            const uid = nanoid(10);
            const collection = firestore().collection(`${name}_week_${count}_customer_details`).doc(uid);
            customerDetails.set(collection, {...user_details[start], uid});
            if(counter_points_500s === 500){
                await customerDetails.commit();
                customerDetails = firestore().batch();
                counter_points_500s = 0;
                remainder_details -= 500;
                // minor increments
                progress_details += 1;
                job.progress(progress_details);
                continue;
            }
            
           }

          var final_progress = progress_details + progress_points;
          if(final_progress === 100){
            job.progress(final_progress);
          }

    }catch(e){
        logger.info('WORKER ERROR', e);
    }
  });
}

// minor changes
// Initialize the clustered worker process
// See: https://devcenter.heroku.com/articles/node-concurrency for more info
throng({ workers, start });