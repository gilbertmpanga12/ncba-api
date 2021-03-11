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
        const url = 'https://firebasestorage.googleapis.com/v0/b/wholesaleduuka-418f1.appspot.com/o/NCBA%2Flite.csv?alt=media&token=a6eb526c-97ea-48e8-8480-f0fd060fc43c';
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
            let progress_details = 0;
            let progress_points = 0;
            // USER POINTS
            const user_points = datas;
            let user_points_length = user_points.length;
            let counter_500s = 0;
            let customerPoints = firestore().batch();
            let remainder = user_points_length;
            for(var start=1; start <= user_points_length; start++){
                counter_500s += 1;
                if(remainder < 500){
                    const uid = nanoid(10);
                    const collection = firestore().collection(`${name}_week_${count}_customer_points`).doc(uid);
                    customerPoints.set(collection, {customerId: user_points[start]['Customer Number'], 
                    loanReference: user_points[start]['Loan Reference'], uid});
                    customerPoints.commit();
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
                    customerPoints.commit();
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
            for(var start=1; start <= user_details_length; start++){
                counter_points_500s += 1;
                if(remainder_details < 500){
                    const uid = nanoid(10);
                    const collection = firestore().collection(`${name}_week_${count}_customer_details`).doc(uid);
                    customerDetails.set(collection, {...user_details[start], uid});
                    customerDetails.commit();// check for last
                    // final increments
                    progress_details = 50;
                    job.progress(progress_details);
                    break;
                }
                const uid = nanoid(10);
                const collection = firestore().collection(`${name}_week_${count}_customer_details`).doc(uid);
                customerDetails.set(collection, {...user_details[start], uid});
                if(counter_points_500s === 500){
                    customerDetails.commit();
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