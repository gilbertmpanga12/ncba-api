let throng = require('throng');
let Queue = require("bull");
const admin = require('firebase-admin');
// const pickRandom = require('pick-random');
const csv = require("csv-stream");
const {logger} = require('./helpers/logger');
const { nanoid } = require('nanoid');
const { firestore } = require('firebase-admin');
const request = require('request');
const progress = require('request-progress');
const options = {
  
    columns : ['Customer Number', 'Loan Reference', 'Loan Start Date', 'Loan Repaid Date']
}
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
  // {redis: {port: 6379, host: '127.0.0.1', password: 'BpJqTatVLvgyUbTVT7Jv4BZLDyX6gaTERTuhlkTBXg3EV8MWjRk5uZI5EzRzR5OoW37lb+ONV8Ev9GOW'}}
  
  
  
  let workQueue = new Queue('work',{redis: {port: 6379, host: '127.0.0.1', password: 'BpJqTatVLvgyUbTVT7Jv4BZLDyX6gaTERTuhlkTBXg3EV8MWjRk5uZI5EzRzR5OoW37lb+ONV8Ev9GOW'}} );

  workQueue.process(maxJobsPerWorker, async (job) => {
    try{
        // const {url, name, count} = job.data;
        const url = 'https://firebasestorage.googleapis.com/v0/b/wholesaleduuka-418f1.appspot.com/o/NCBA%2F1615411998226_Test%20file%208.csv?alt=media&token=bb9cac79-b72c-4fbf-9d1f-005491c79f53';
        const name = 'Holla';
        const count = 8;
        let datas = [];
        // const name = job.data['name'];
        // const count = job.data['count'];
        // const datas = job.data['user_details'];
        const csvStream = csv.createStream(options);
        progress(request(url)).on('progress', function (state) {
            console.log('progress', state);
        }).pipe(csvStream).on('error',function(err){
                console.error(err);
            })
            .on('data', function(csv_data){
               datas.push({
            'Customer Number': csv_data['Customer Number'],
            'Loan Reference': csv_data['Loan Reference'],
            'Loan Repaid Date': csv_data['Loan Repaid Date'],
            'Loan Start Date': csv_data['Loan Start Date']
        });
               
        }).on('end',function(data){
            // let points_500 = 500;
            let counter_break = 0;
            let all_count_length = datas.length;
            let customerPoints = firestore().batch();
            let customerDetails = firestore().batch();
            datas.forEach(customer_data => {
                counter_break += 1;
                if(counter_break === 500){
                    const payload = datas.splice(counter_break, all_count_length);
                    payload.forEach(user_data => {
                    const uid = nanoid(10);
                    console.log(user_data)
                    const points =  firestore().collection(`${name}_week_${count}_customer_points`).doc(uid); // points
                    customerPoints.set(points, {customerId: user_data['Customer Number'], 
                    loanReference: user_data['Loan Reference'], uid});

                    const details = firestore().collection(`${name}_week_${count}_customer_details`).doc(uid); // details
                    customerDetails.set(details, {...user_data, uid});

                    });
                    customerPoints.commit();
                    customerDetails.commit();
                    customerPoints = firestore().batch();
                    customerDetails = firestore().batch();
                    counter_break = 0;
                    // final increments
                    // progress_points = 50;
                    //job.progress(progress_points);
                }
            });
        });
       

    }catch(e){
        logger.info('WORKER ERROR', e);
    }
  });
}

// minor changes
// Initialize the clustered worker process
// See: https://devcenter.heroku.com/articles/node-concurrency for more info
throng({ workers, start });