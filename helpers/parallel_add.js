const admin = require('firebase-admin');
const pickRandom = require('pick-random');


async function ParallelIndividualWrites(datas, res) {
    try{
        const collection = admin.firestore().collection(`week_${count}_customer_points`);
        await Promise.all(datas.map((data) => collection.doc(data['uid']).set({customerId: data['Customer Number'], 
        loanReference: data['Loan Reference']})));
        res.status(200).send({message: 'Succefully added all customer ids'});
    }catch(e){
        console.log('FAILED TO ADD CUSTOMER IDS', e);
        res.status(500).send({message: 'FAILED TO ADD CUSTOMER IDS'});
    }
}

async function WriteCustomerDetails(datas, res) {
    try{
        const collection = admin.firestore().collection('weeklyDrawsDetails');
        await Promise.all(datas.map((data) => collection.add(data)));
        res.status(200).send({message: 'Succefully added all customer ids'});
    }catch(e){
        console.log('FAILED TO ADD CUSTOMER IDS', e);
        res.status(500).send({message: 'FAILED TO ADD CUSTOMER IDS'});
    }
}

 async function RandomiseLuckyWinners(res){
    try{
        const collection = admin.firestore().collection(`week_${count}_customer_points`);
        const doc = await collection.get();
        const results = [];
        doc.forEach(doc => {
            results.push(doc.data());
        });
        const luckyWinners = pickRandom(results, {count: 10});
        const weekCount = 1;
        storeRandomisedWinners(weekCount, luckyWinners);
        res.status(200).send({message: luckyWinners});
    }catch(e){
        console.log(e);
        res.status(500).send({message: 'Please check whether the data entered was valid or contact support'});
    }
}


async function storeRandomisedWinners(week_count, luckyWinners, name){
    try{
    const collection = admin.firestore().collection(`winners_${name}_${week_count}`).doc(week_count);
    collection.set(luckyWinners);
    clusterWeeklyLoosers(luckyWinners);
    }catch(e){
       console.log(e);  
    }
}

async function clusterWeeklyLoosers(luckyWinners){
    try{
        const collection = admin.firestore().collection(`week_${count}_customer_points`);
        await Promise.all(luckyWinners.map((winner) => {
            collection.where('Customer Number', '==', winner['Customer Number']).get().then((winner_id) => {
                winner_id.forEach(x => {
                    admin.firestore().collection(`week_${count}_customer_points`).doc(x.id).delete()
                });
            })
        }));
        res.status(200).send({message: 'Succefully added all customer ids'});
    }catch(e){
        console.log('FAILED TO ADD CUSTOMER IDS', e);
        res.status(500).send({message: 'FAILED TO ADD CUSTOMER IDS'});
    }
}

async function enterGrandDraw(uid, name, res){
    try{
    const week_count_total = await admin.firestore().collection(`${name}_week_count_total`).doc(uid).get();
    if (!week_count_total.exists) {
        console.log('No such document!');
      } else {
        res.status(200).send({message: doc.data()});
      }
    }catch(e){
        res.status(500).send({message: e});
    }
}

async function generateRaffleProject(uid, count, name, res){
    try{
    await admin.firestore().collection(`${name}_week_count_total`).doc(uid).set({
        week_count_total: count
    });
    res.status(200).send({message: 'Created Successfully'});
    }catch(e){
        res.status(500).send({message: e});
    }
}


module.exports = {ParallelIndividualWrites, RandomiseLuckyWinners, WriteCustomerDetails, 
    generateRaffleProject, enterGrandDraw};