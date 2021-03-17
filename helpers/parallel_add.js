require('dotenv').config();
const admin = require('firebase-admin');
const pickRandom = require('pick-random');
const {logger} = require('../helpers/logger');
const { nanoid } = require('nanoid');
const { firestore } = require('firebase-admin');

let Queue = require('bull');
// ********
let workQueue = new Queue('work', {redis: {port: 6379, 
    host: '127.0.0.1', password: process.env.REDIS_PASSWORD_RAFFLE}});

async function ParallelIndividualWrites(url, count, res, name, operation, weekDuration) {
    try{
        const job =  await workQueue.add({url, count, name, operation, weekDuration});
        res.status(200).send({message: 'Succefully added all customer ids: ' + job.id, jobId: job.id});
        logger.info(`Job ID ${job.id}`);
  }catch(e){
        logger.info('FAILED TO ADD CUSTOMER IDS', e);
        res.status(500).send({message: 'An error occured while queing'});
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
        logger.info('Clustering completed');
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
    const {jobId} = req.body;
    let job = await workQueue.getJob(jobId);
  
    if (job === null) {
      res.status(404).end();
    } else {
      let state = await job.getState();
      let progress = job._progress;
      let reason = job.failedReason;
      res.json({ jobId, state, progress, reason });
    }
  }



async function updateWeeklyState(count, name){
  await firestore().collection('all_projects')
  .doc(name).collection('week_states').doc(`week_${count}`).update({state: false});

}

workQueue.on('global:completed', (jobId, result) => {
    console.log(`Job completed with result ${result}`);
  });


module.exports = {ParallelIndividualWrites, RandomiseLuckyWinners,
     enterGrandDraw, clusterWeeklyLoosers, 
     AddWeekStates, getJobId, ParallelIndividualWrites, 
     WriteCustomerDetails, updateWeeklyState};