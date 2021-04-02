require("dotenv").config();
const admin = require("firebase-admin");
const pickRandom = require("pick-random");
const { logger } = require("../helpers/logger");
const { firestore } = require("firebase-admin");


let Queue = require("bull");

let workQueue = new Queue("work", {
  redis: {
    port: 6379,
    host: "127.0.0.1",
    password:
      process.env.REDIS_PASSWORD_RAFFLE,
  },
});

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
    const doc = await collection.get();
    const results = [];
    doc.forEach((doc) => {
      results.push(doc.data());
    });
    const luckyWinners = pickRandom(results, { count: 10 });
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
    clusterWeeklyLoosers(luckyWinners, count, name, weekDuration);
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
        const uid = winner["Loan Reference"];
        customer_details.doc(uid).delete();
      })
    ).then((res) => logger.info("cleaned winners", res));

    if (count == weekDuration) {
      console.log("week durantion has been called", count == weekDuration);
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
      setDocumentCount(name, count, docsCount);
      return;
    }

    if(progress['operationType'] === 'DELETION' && 
    progress['current'] === progress['remaining']){
    updateWeeklyState(count, name);
    setDocumentCount(name, count, 0);
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

async function setDocumentCount(name, count, docsCount){
    try{
        const detailsCounter = firestore().collection(`${name}_week_${count}_counter`).doc(`${count}`);
        await detailsCounter.set({current_count: docsCount});
    }catch(e){
        logger.info('Failed to reset counter after deleting collection', e);
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
      const uid = `${csv_doc["Loan Reference"]}`.trim();
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
      const uid = `${csv_doc["Loan Reference"]}`.trim();
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
