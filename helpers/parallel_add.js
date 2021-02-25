const admin = require('firebase-admin');
const pickRandom = require('pick-random');
const {logger} = require('../helpers/logger');
  


async function ParallelIndividualWrites(datas,count, res, name) {
    try{
        const collection = admin.firestore().collection(`${name}_week_${count}_customer_points`);
        await Promise.all(datas.map((data) => collection.doc(data['uid']).set({customerId: data['Customer Number'], 
        loanReference: data['Loan Reference']})));
        res.status(200).send({message: 'Succefully added all customer ids'});
    }catch(e){
        logger.info('FAILED TO ADD CUSTOMER IDS', e);
        res.status(500).send({message: 'FAILED TO ADD CUSTOMER IDS'});
    }
}

async function WriteCustomerDetails(datas,count, res, name) {
    try{
        const collection = admin.firestore().collection(`${name}_week_${count}_customer_details`);
        await Promise.all(datas.map((data) => collection.add(data)));
        res.status(200).send({message: 'Succefully added all customer ids'});
    }catch(e){
        logger.info('FAILED TO ADD CUSTOMER IDS', e);
        res.status(500).send({message: 'FAILED TO ADD CUSTOMER IDS'});
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
        console.log(luckyWinners)
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
        const collection = admin.firestore().collection(`${name}_week_${count}_customer_points`);
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

async function enterGrandDraw(uid, name, res){
    try{
    const week_count_total = await admin.firestore().collection(`${name}_week_count_total`).doc(uid).get();
    if (!week_count_total.exists) {
        logger.info('No such document!');
      } else {
        res.status(200).send({message: doc.data()});
      }
    }catch(e){
        res.status(500).send({message: e});
    }
}




module.exports = {ParallelIndividualWrites, RandomiseLuckyWinners, WriteCustomerDetails, 
     enterGrandDraw, clusterWeeklyLoosers};