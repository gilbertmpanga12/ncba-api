require("dotenv").config();
const admin = require("firebase-admin");
const pickRandom = require("pick-random");
const { logger } = require("../helpers/logger");
const { firestore } = require("firebase-admin");
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

let workQueue = new Queue("work", productionRedis);

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
    const collection = admin
      .firestore()
      .collection(`${name}_week_${count}_customer_details`);
    const doc = await collection.limit(80000).get();
    const results = [];
    doc.forEach((doc) => {
      results.push(doc.data());
    });
    const luckyWinners = pickRandom(results.reverse(), { count: 10 });
    storeRandomisedWinners(count, luckyWinners, name, weekDuration);

    res.status(200).send({ message: luckyWinners });
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

async function storeRandomisedWinners(count, luckyWinners, name, weekDuration) {
  try {
    await admin
      .firestore()
      .collection(`${name}_week_${count}_winners`)
      .doc(`${count}`)
      .set({ winners: luckyWinners }, { merge: true });
      await reduceBy10AfterRandomization(name, count);
      // disable randomizaition
    await clusterWeeklyLoosers(luckyWinners, count, name, weekDuration);
  } catch (e) {
    logger.info("Store randomised luck winners error", e);
  }
}

async function clusterWeeklyLoosers(luckyWinners, count, name, weekDuration) {
  try {
    const customer_details = admin
      .firestore()
      .collection(`${name}_week_${count}_customer_details`);
    await Promise.all(
      luckyWinners.map((winner) => {
        const uid = winner['Customer Number'];
        customer_details.doc(uid).delete();
      })
    ).then((res) => logger.info("cleaned winners", res));

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
          await detailsCounter.set({current_count: docsCount}, {merge: true});
        }

        return;
      }
      
      if(Number(count) > 1 && operationType === "DATA_CREATION"){
       const diff = Number(count) - 1;
       const oldCountStore = firestore().collection(`${name}_week_${diff}_counter`).doc(`${diff}`);
       const getOldCount = await oldCountStore.get();
       let _oldWeekCount = getOldCount.exists ? Number(getOldCount.data().current_count): 0;

       const newWeekCollection = await firestore().collection(`${name}_week_${count}_counter`)
       .doc(`${count}`);
       const newWeekCount = await newWeekCollection.get();
       if(!newWeekCount.exists || newWeekCount.data()['current_count'] === 0){
       await newWeekCollection.set({current_count: (docsCount + _oldWeekCount)}, {merge: true});
       }else{
        await newWeekCollection.set({current_count: (Number(newWeekCount.data().current_count) 
          + docsCount)}, {merge: true});
       }

      

      //  const customerDetails = await firestore().collection(`${name}_week_${diff}_customer_details`)
      //   .limit(7).get();
      //  const datas = [];
      //  customerDetails.forEach(customer_details => datas.push(customer_details.data()));
      //  await storeMoreCustomerData(datas, name, count);

       return;

      }

  }catch(e){
      logger.info('Failed to reset counter after deleting collection', e);
  }
  }



    async function storeMoreCustomerData(datas, name, count){
      //const operation = "DATA_CREATION";
      const documentSnapshotArray = datas;
      //const total_count = datas.length;
      const batchArrayDetails = [];
      batchArrayDetails.push(firestore().batch());
      let operationCounter = 0;
      let batchIndex = 0;
      documentSnapshotArray.forEach((csv_doc) => {
        const uid = `${csv_doc['Customer Number']}`;
        const documentDataDetails = firestore().collection(`${name}_week_${count}_customer_details`).doc(uid);
        batchArrayDetails[batchIndex].set(documentDataDetails, {...csv_doc});
        operationCounter++;
        
    
        if (operationCounter === 499) {
          batchArrayDetails.push(firestore().batch());
          batchIndex++;
          operationCounter = 0;
        }
      });
    
      batchArrayDetails.forEach(async (batch) => await batch.commit());
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
  deleteLucky3
};
