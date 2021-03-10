
const admin = require('firebase-admin');
const pickRandom = require('pick-random');
const {logger} = require('../helpers/logger');
const csv = require("csvtojson");
const { nanoid } = require('nanoid');
const { firestore } = require('firebase-admin');

let Queue = require('bull');
// Connect to a local redis intance locally, and the Heroku-provided URL in production
let REDIS_URL = process.env.HEROKU_REDIS_GOLD_URL || 'redis://127.0.0.1:6379';

let workQueue = new Queue('work', REDIS_URL);

async function ParallelIndividualWrites(datas,count, res, name) {
    try{
         let csvResults = datas;
         const csvRows = await csv({
            noheader:false,
            output: "json",
            ignoreEmpty: true
        })
        .fromString(csvResults.data.toString());
        const user_details = csvRows;

        let job = await workQueue.add({user_details, count, name});
        res.status(200).send({message: 'Succefully added all customer ids: ' + job.id});
        console.log(`Job ID ${job.id}`);
        
       
  }catch(e){
        logger.info('FAILED TO ADD CUSTOMER IDS', e);
        res.status(500).send({message: 'FAILED TO ADD CUSTOMER IDS'});
    }
}

async function AddWeekStates(name, datas, res) {
    try{
        const collection = admin.firestore().collection('all_projects');
        await Promise.all(datas.map((data) => {
             let payload = {};
             payload['state'] = false;
             payload['week'] = parseInt(data.replace( /^\D+/g, ''));
            collection.doc(name).collection('week_states').doc(data).set(payload);
        }));
        
        await Promise.all(datas.map((data) => {
            let payload = {};
            payload['state'] = false;
            payload['week'] = parseInt(data.replace( /^\D+/g, ''));
           collection.doc(name).collection('week_state_draw').doc(data).set(payload);
       }));
        res.status(200).send({message: 'Succefully added all WEEK STATES'});
    }catch(e){
        logger.info('FAILED TO ADD WEEK STATES', e);
        res.status(500).send({message: 'FAILED TO ADD ADD WEEK STATES'});
    }
}

async function WriteCustomerPoints(datas,name,count){
    try{
        const user_details = datas;
        let user_details_length = user_details.length;
        let counter_500s = 0;
        let customerPoints = firestore().batch();
        let remainder = user_details_length;
        for(var start=0; start <= user_details_length; start++){
            counter_500s += 1;
            if(remainder < 500){
                const uid = nanoid(10);
                const collection = firestore().collection(`${name}_week_${count}_customer_points`).doc(uid);
                customerPoints.set(collection, {customerId: user_details[start]['Customer Number'], 
                loanReference: user_details[start]['Loan Reference'], uid});
                await customerPoints.commit();
                resolve(500);
                break;
            }

            const uid = nanoid(10);
            const collection = firestore().collection(`${name}_week_${count}_customer_points`).doc(uid);
            customerPoints.set(collection, {customerId: user_details[start]['Customer Number'], 
            loanReference: user_details[start]['Loan Reference'], uid});
            if(counter_500s === 500){
                await customerPoints.commit();
                customerPoints = firestore().batch();
                counter_500s = 0;
                remainder -= 500;
                continue;
            }
            
           }
    }catch(e){
        logger.info('FAILED TO ADD Customer points', e);
    }
}

async function WriteCustomerDetails(datas,name,count) {
    try{
        const user_details = datas;
        let user_details_length = datas.length;
        let counter_500s = 0;
        var customerDetails = firestore().batch();
        let remainder = user_details_length;
        for(var start=0; start <= user_details_length; start++){
            counter_500s += 1;
            if(remainder < 500){
                const uid = nanoid(10);
                const collection = firestore().collection(`${name}_week_${count}_customer_details`).doc(uid);
                customerDetails.set(collection, {...user_details[start], uid});
                await customerDetails.commit();// check for last
                break;
            }
            const uid = nanoid(10);
            const collection = firestore().collection(`${name}_week_${count}_customer_details`).doc(uid);
            customerDetails.set(collection, {...user_details[start], uid});
            if(counter_500s === 500){
                await customerDetails.commit();
                customerDetails = firestore().batch();
                counter_500s = 0;
                remainder -= 500;
                continue;
            }
            
           }
      
    }catch(e){
        logger.info('FAILED TO ADD CUSTOMER DETAILS', e);
    }
}

 async function RandomiseLuckyWinners(name, count, res){
    try{
        console.log(`${name}_week_${count}_customer_points`)
        const collection = admin.firestore().collection(`${name}_week_${count}_customer_points`);
        const doc = await collection.get();
        const results = [];
        doc.forEach(doc => {
            results.push(doc.data());
        });
        console.log(results)
        const luckyWinners = pickRandom(results, {count: 10});
        storeRandomisedWinners(count, luckyWinners, name);
        
        res.status(200).send({message: luckyWinners});
    }catch(e){
        logger.info(e);
        res.status(500).send({message: 'Please check whether the data entered was valid or contact support'});
    }
}


async function storeRandomisedWinners(count, luckyWinners, name){
    try{
        
     await admin.firestore().collection(`${name}_week_${count}_winners`).doc(`${count}`)
     .set({winners:luckyWinners}, {merge: true});
     clusterWeeklyLoosers(luckyWinners, count, name);
    }catch(e){
        logger.info(e); 
    }
}

async function clusterWeeklyLoosers(luckyWinners, count, name){
    try{
        const collection =  admin.firestore().collection(`${name}_week_${count}_customer_points`);
        await Promise.all(luckyWinners.map((winner) => {
            collection.where('customerId', '==', winner['customerId']).get().then((winner_id) => {
                winner_id.forEach(x => {
                    admin.firestore().collection(`${name}_week_${count}_customer_point`).doc(x.id).delete()
                });
            })
        }));
    }catch(e){
        logger.info(e);
    }
}

async function enterGrandDraw(name, count , res){
    try{
        let start = 1;
        let startDetails = 1;
        let customerPoints = [];
        let customerDetails = [];
        let total = count;
        let totalDetails = count;
        while(start <= total){
            customerPoints.push(`${name}_week_${start}_customer_points`);
            start++;
        }
        
        while(startDetails <= totalDetails){
            customerDetails.push(`${name}_week_${startDetails}_customer_details`);
            startDetails++;
        }
        const grandPoints = admin.firestore().collection(`${name}_grand_total_points`);
        const grandDetails = admin.firestore().collection(`${name}_grand_total_details`);
        
        await Promise.all(customerPoints.map((points) => {
            admin.firestore().collection(points).get().then((collection_points) => {
                collection_points.forEach(x => {
                    grandPoints.doc(x.id).set(x.data())
                });
            })
        }));
        
        
        await Promise.all(customerDetails.map((details) => {
            admin.firestore().collection(details).get().then((collection_details) => {
                collection_details.forEach(x => {
                    grandDetails.doc(x.id).set(x.data())
                });
            })
        }));
        
        res.status(200).send({message: `Generated grand draw successfully`});
        
    }catch(e){
        res.status(500).send({message: e});
    }
}


async function getJobId(req, res){
    let id = req.params.id;
    let job = await workQueue.getJob(id);
  
    if (job === null) {
      res.status(404).end();
    } else {
      let state = await job.getState();
      let progress = job._progress;
      let reason = job.failedReason;
      res.json({ id, state, progress, reason });
    }
  }

workQueue.on('global:completed', (jobId, result) => {
    console.log(`Job completed with result ${result}`);
  });
  


module.exports = {ParallelIndividualWrites, RandomiseLuckyWinners,
     enterGrandDraw, clusterWeeklyLoosers, AddWeekStates, getJobId, ParallelIndividualWrites, WriteCustomerDetails};