require("dotenv").config();
const admin = require("firebase-admin");
const pickRandom = require("pick-random");
const { logger } = require("../helpers/logger");
const { firestore } = require("firebase-admin");
const openDatabase = require('../utilities/mongo_client');
const progress = require("request-progress");
const csv = require("csv-stream");
const request = require("request");

const productionRedis = {
  redis: {
    port: 6379,
    host: "127.0.0.1",
    password:
      process.env.REDIS_PASSWORD_RAFFLE,
  },
};
const developmentRedis =  "redis://127.0.0.1:6379";

let Queue = require("bull");

let workQueue = new Queue("work", developmentRedis);

async function ParallelIndividualWrites(
  url,
  count,
  res,
  name,
  operation,
  weekDuration,
  csv_file,
  startDate,
  endDate
) {
  try {
    const job = await workQueue.add({
      url,
      count,
      name,
      operation,
      weekDuration,
      csv_file,
      startDate,
      endDate,
    });
    res
      .status(200)
      .send({
        message: "Succefully added all customer ids: " + job.id,
        jobId: job.id,
      });
    logger.info(`Job ID ${job.id}`);
  } catch (e) {
    logger.info("FAILED TO ADD CUSTOMER IDS", e);
    res.status(500).send({ message: "An error occured while queing" });
  }
}

async function AddWeekStates(name, datas, res) {
  try {
    const collection = admin.firestore().collection("all_projects");
    await Promise.all(
      datas.map((data) => {
        let payload = {};
        payload["state"] = false;
        payload["week"] = parseInt(data.replace(/^\D+/g, ""));
        collection.doc(name).collection("week_states").doc(data).set(payload);
      })
    );

    await Promise.all(
      datas.map((data) => {
        let payload = {};
        payload["state"] = false;
        payload["week"] = parseInt(data.replace(/^\D+/g, ""));
        collection
          .doc(name)
          .collection("week_state_draw")
          .doc(data)
          .set(payload);
      })
    );
    res.status(200).send({ message: "Succefully added all WEEK STATES" });
  } catch (e) {
    logger.info("FAILED TO ADD WEEK STATES", e);
    res.status(500).send({ message: "FAILED TO ADD ADD WEEK STATES" });
  }
}

async function RandomiseLuckyWinners(name, count, weekDuration, res) {
  try {
    openDatabase(`${name}_week_${count}_customer_details`,
      `${name}_week_${count}_migration`).then(client => {
        let unionCollections = [];
        let pipeline;
        if(count > 1){
         
        for(let start=count-1;start >= 1; start--){
            unionCollections.push(
                { "$unionWith": {
                    "coll": `${name}_week_${start}_migration`,
                    "pipeline": [{
                        "$project": { 
                            "Customer Number": true, 
                            "Loan Reference": true , 
                            "Loan Repaid Date": true, 
                            "Loan Start Date": true,
                            "week": true,
                            "_id": 0}
                    }]
                } }
        );
        }
        pipeline = [
            { "$project": { 
                "Customer Number": true, 
                "Loan Reference": true , 
                "Loan Repaid Date": true, "Loan Start Date": true, "week": true, "_id": 0} },
                ...unionCollections,
                {"$sample": {"size": 10}}
        ];
        }
       const filter = [{"$sample": {"size": 10}}];
       const db = count > 1 ? client.collection.aggregate(pipeline): client.collection.aggregate(filter);
        db.toArray().then(result => {
          const luckyWinners = result.map(function(csv_doc){
            return {
              "Customer Number":csv_doc["Customer Number"],
              "Loan Reference": csv_doc["Loan Reference"],
              "Loan Repaid Date": csv_doc["Loan Repaid Date"],
              "Loan Start Date": csv_doc["Loan Start Date"],
              "week": csv_doc["week"]
            };
          });
          storeRandomisedWinners(count, luckyWinners, name, weekDuration);
          res.status(200).send({ message: luckyWinners });
        });
        
      });
  
  } catch (e) {
    logger.info("Randomise lucky winners error", e);
    res
      .status(500)
      .send({
        message:
          "Please check whether the data entered was valid or contact support",
      });
  }
}

async function storeParticpantsArray(name, participantsStore){
  try{
    await firestore().collection(`${name}_all_participants_count`).doc(name).set({
      participantsStore
    });
  }catch(e){
    return e;
  }
}

async function storeRandomisedWinners(count, luckyWinners, name, weekDuration) {
  try {
    await admin
      .firestore()
      .collection(`${name}_week_${count}_winners`)
      .doc(`${count}`)
      .set({ winners: luckyWinners }, { merge: true });
      const detailsCounter = await firestore().collection(`${name}_week_${count}_counter`).doc(`${count}`).get();
      const all_participants = await firestore().collection(`${name}_all_participants_count`).doc(name).get();
      
    await clusterWeeklyLoosers(luckyWinners, count, name, weekDuration);
  } catch (e) {
    logger.info("Store randomised luck winners error", e);
  }
}

async function clusterWeeklyLoosers(luckyWinners, count, name, weekDuration) {
  try {
    let collection = `${name}_week_${count}_migration`;
    const pastWinners =  await admin
    .firestore()
    .collection(`${name}_week_${Number(count) - 1}_winners`)
    .doc(`${Number(count) - 1 }`);
    const winners = await pastWinners.get();

    if(Number(count) === 1){
      luckyWinners.forEach(async (doc) => {
        await (await openDatabase(`${name}_week_${doc['week']}_customer_details`,
      `${name}_week_${doc['week']}_migration`)).migration.deleteOne({"_id": doc["Loan Reference"]});
      });
    } 


    if(Number(count) > 1 && winners.exists){
      const _winners = winners.data()['winners'];
      _winners.forEach(async (doc) => {
        await (await openDatabase(`${name}_week_${doc['week']}_customer_details`,
      `${name}_week_${doc['week']}_migration`)).migration.deleteOne({"_id": doc["Loan Reference"]});
      });
    }

    const customer_details = admin
      .firestore()
      .collection(collection);
      
   
    await firestore().collection('all_projects')
    .doc(name).collection('week_state_draw')
    .doc(`week_${count}`).set({randomised: true}, {merge: true});

    if (count == weekDuration) {
      const job = await workQueue.add({
        generateLucky10: true,
        count: count,
        name: name,
      });
      logger.info("Clustering completed " + job.id);
    }
  } catch (e) {
    logger.info("clustering error", e);
  }
}

async function getJobId(req, res) {
  const { jobId } = req.body;
  let job = await workQueue.getJob(jobId);

  if (job === null) {
    res.status(404).end();
  } else {
    let state = await job.getState();
    let progress = job._progress;
    let reason = job.failedReason;
    const {name, count, docsCount} = progress;
    res.json({ jobId, state, progress, reason });

    if(progress['operationType'] === 'DATA_CREATION' && 
    progress['current'] === progress['remaining']){
      setDocumentCount(name, count, docsCount, 'DATA_CREATION');
      return;
    }

    if(progress['operationType'] === 'DELETION' && 
    progress['current'] === progress['remaining']){
    updateWeeklyState(count, name);
    setDocumentCount(name, count, 0, 'DELETION');
    deleteLucky3(name);
      return;
    }

  }
}

async function updateWeeklyState(count, name) {
  await firestore()
    .collection("all_projects")
    .doc(name)
    .collection("week_states")
    .doc(`week_${count}`)
    .update({ state: false }, { merge: true });
}

async function setDocumentCount(name, count, docsCount, operationType){
  try{
      if(Number(count) === 1 && operationType === "DATA_CREATION"){
        const detailsCounter = firestore().collection(`${name}_week_${count}_counter`).doc(`${count}`);
        const checkIfExits = await detailsCounter.get();
        if(checkIfExits.exists){
          const oldValue = Number(checkIfExits.data().current_count);
          await detailsCounter.set({current_count: (oldValue + docsCount)}, {merge: true});
        }else{
        await detailsCounter.set({current_count:docsCount}, {merge: true});
        }

        return;
      }
      
      if(Number(count) > 1 && operationType === "DATA_CREATION"){
       const diff = Number(count) - 1;
       const oldCountStore = firestore().collection(`${name}_week_${diff}_counter`).doc(`${diff}`);
       const newWeekCollection = await firestore().collection(`${name}_week_${count}_counter`)
       .doc(`${count}`);
       const newWeekCount = await newWeekCollection.get();
       if(!newWeekCount.exists || newWeekCount.data()['current_count'] === 0){

        openDatabase(`${name}_week_${Number(count)}_customer_details`,
    `${name}_week_${count}_migration`).then(client => {
        const unionCollections = [];
        for(let start=count-1;start >= 1; start--){
            unionCollections.push(
                { "$unionWith": {
                    "coll": `${name}_week_${start}_migration`,
                    "pipeline": [{
                        "$project": { 
                            "Customer Number": true, 
                            "Loan Reference": true , 
                            "Loan Repaid Date": true, 
                            "Loan Start Date": true,
                            "_id": 0}
                    }]
                } }
        );
        }
        const pipeline = [
            { "$project": { 
                "Customer Number": true, 
                "Loan Reference": true , 
                "Loan Repaid Date": true, "Loan Start Date": true, "_id": 0} },
                ...unionCollections,
                {"$count": "totalCount"}
        ];
       const report = client.collection.aggregate(pipeline, (err, res) => {
        if(err) throw err;
        res.forEach(cb => {
          if(cb.totalCount){
            newWeekCollection.set({current_count: cb.totalCount}, {merge: true}).then(() => null);
          }
        });
       });
       
    })
       
       }else{
        await newWeekCollection.set({current_count: (Number(newWeekCount.data().current_count) 
          + docsCount)}, {merge: true});
       }
       return;

      }

  }catch(e){
      logger.info('Failed to reset counter after deleting collection', e);
  }
  }


    async function reduceBy10AfterRandomization(name, count){
      try{
      const currentWeek = firestore().collection(`${name}_week_${count}_counter`).doc(`${count}`);
      const getWeekData = await currentWeek.get();
      if(getWeekData.exists){
        const count = Number(getWeekData.data().current_count);
        await currentWeek.set({current_count: (count - 10)},{merge: true});
      }

      }catch(e){
        logger.info("Failed to reduce weekly randomised customers");
      }
    }


// async function updateWeeklyStateRandom(count, name, csv_file, startDate, endDate){
//     firestore().collection('all_projects')
//     .doc(name).collection('week_state_draw').doc(`week_${count}`).set({state: true,
//       startedDate:startDate, endDate:endDate, csv_file: csv_file}, {merge: false});
//   }

async function pickLucky3(name, res) {
  try {
    let winners = [];
    const weeklyGrand = await firestore()
      .collection(`${name}_grand_total_details`)
      .get();
    weeklyGrand.forEach((data) => winners.push(data.data()));
    let lucky3 = pickRandom(winners, { count: 3 });
    // details
    lucky3.forEach((csv_doc) => {
      const uid = `${csv_doc['Customer Number']}`;
      firestore()
        .collection(`${name}_winner3_details`)
        .doc(uid)
        .set(csv_doc)
        .then((x) => x.writeTime);
    });
    res.status(200).send({ message: lucky3 });
    
  } catch (e) {
    res.status(500).send({ message: "Failed to randomise lucky 3 winners" });
    logger.info("Failed to pick lucky 3", e);
  }
}

 function validateCsvFile(url, res){
  const csvStream = csv.createStream();
    var duplicateCount = {};
    var  errorTypeMessage;
    progress(request(url))
    .on("progress", function (state) {
      logger.info("progress", state);
    })
    .pipe(csvStream)
    .on("error", function (err) {
      logger.error(err);
      job.progress('Oops an internal error occured, please contact support');
    })
    .on("data", function (csv_data) {
      try{
        if(csv_data["Customer Number"].trim() && 
        csv_data["Loan Reference"].trim() && 
        csv_data["Loan Repaid Date"].trim() 
        && csv_data["Loan Start Date"].trim()){
          // check for duplicates
          const key = csv_data["Loan Reference"].trim();
    
          if(duplicateCount[key] === 0){
            duplicateCount[key]++;
                      }else{
                        duplicateCount[key] = 0;
                      }
          
                    if(duplicateCount[key] >= 1){
                      console.log("DUPLICATES")
                        const eror_message = `Please check your csv file for duplicates`;
                        errorTypeMessage =  eror_message;
                        throw eror_message;
                      }
                    logger.info(csv_data);
        }else{
          const eror_message = `Please check your csv file for missing 
          blank customer numbers and empty fields`;
          errorTypeMessage =  eror_message;
          throw eror_message;
        }
      }catch(e){
        console.log("GENERAAL ERROR!!");
        if(errorTypeMessage){
          res.status(200).send({message: errorTypeMessage, status: 'mixed_errors'});
          return;
        }
        const eror_message = `Please make sure the file has correct cells named`;
        res.status(200).send({message: errorTypeMessage, status: 'mixed_errors'});
      }
    })
    .on("end", async function (data) {
     if(!errorTypeMessage) res.status(200).send({message: "clean", status: 'clean'});
    });
}



async function deleteLucky3(name){
  try{
    let winners = [];
    const weeklyGrand = await firestore()
      .collection(`${name}_grand_total_details`)
      .get();
    weeklyGrand.forEach((data) => winners.push(data.data()));
    let lucky3 = pickRandom(winners, { count: 3 });
    // details
    lucky3.forEach((csv_doc) => {
      const uid = `${csv_doc['Customer Number']}`;
      firestore()
        .collection(`${name}_winner3_details`)
        .doc(uid)
        .delete();
        
    });
  }catch(e){
      logger.info('failed to delete lucky 3', e);
  }
}

async function currentWeek(count, name, docsCount, page, res) {
  try {
    /*
    data: any[];
  draw: number;
  recordsFiltered: number;
  recordsTotal: number;
    */
    const resp = [];
    const first = firestore()
      .collection(`${name}_week_${count}_customer_details`)
      .orderBy("Loan Reference")
      .limit(docsCount)
      .offset(page);
    const process_results = await first.get();
    process_results.forEach((x) => resp.push(x.data()));

    res.status(200).send({ data: resp, draw:1, recordsFiltered, recordsTotal});
  } catch (e) {
    res.status(500).send({ message: "something went wrong" });
    console.log("Current week pagination error", e);
  }
}

workQueue.on("global:completed", (jobId, result) => {
  console.log(`Job completed with result ${result}`);
});

module.exports = {
  ParallelIndividualWrites,
  RandomiseLuckyWinners,
  clusterWeeklyLoosers,
  AddWeekStates,
  getJobId,
  ParallelIndividualWrites,
  updateWeeklyState,
  pickLucky3,
  currentWeek,
  setDocumentCount,
  pickLucky3,
  deleteLucky3,
  validateCsvFile
};
