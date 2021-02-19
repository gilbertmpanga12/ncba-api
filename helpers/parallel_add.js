const admin = require('firebase-admin');
const pickRandom = require('pick-random');


async function ParallelIndividualWrites(datas, res) {
    try{
        const collection = admin.firestore().collection('customerPoints');
        await Promise.all(datas.map((data) => collection.add(data)));
        res.status(200).send({message: 'Succefully added all customer ids'});
    }catch(e){
        console.log('FAILED TO ADD CUSTOMER IDS');
        res.status(500).send({message: 'FAILED TO ADD CUSTOMER IDS'});
    }
}

 async function RandomiseLuckyWinners(res){
    try{
        const collection = admin.firestore().collection('customerPoints');
        const doc = await collection.get();
        const results = [];
        doc.forEach(doc => {
            results.push(doc.data());
        });
        const luckyWinners = pickRandom(results, {count: 10});
        res.status(200).send({message: luckyWinners});
    }catch(e){
        console.log(e);
        res.status(500).send({message: e});
    }
}

module.exports = {ParallelIndividualWrites, RandomiseLuckyWinners};