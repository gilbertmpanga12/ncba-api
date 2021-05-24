const { logger } = require('../helpers/logger');
const openDatabase = require('../utilities/mongo_client');
const stream = require('stream');
const writeToFile = require('../utilities/create_report');
const expirydate = {action: 'read', expires: '03-09-2500'};
const firebase = require('firebase-admin');
const google_storage_bucket = "ncba-313413.appspot.com";


function generateReport(name,count, generateEntireReport, res){
    if(Number(count) > 1){
        queryAdditionalWeeks(name, count, generateEntireReport, res);
        return;
    }
    queryFirstWeek(name, count, res);
}


function queryFirstWeek(name, count, res){
    const outputpath = __dirname + `/${name}_week_${count}_batch.csv`;
    const readStream = new stream.Readable({objectMode: true});
    readStream._read = () => {};
    readStream.pipe(writeToFile(outputpath, res, false)).on("finish", () => uploadToStorage(outputpath, res));
    openDatabase(`${name}_week_${Number(count)}_customer_details`,
    `${name}_week_${Number(count)}_migration`).then(client => {
        const cursor = client.collection.find({}, {fields: {_id:0}});
        return readDatabaseCursor(cursor, outputpath, res, readStream).then(() => client.close());
    }).catch(err => {
        logger.info(err);
    });
}

// readdatabse cursor for first week
function readDatabaseCursor(cursor, path, res, readStream){
    return cursor.next().then(record => {
        if(record){
            readStream.push(record);
            return readDatabaseCursor(cursor, path, res, readStream);
        }else{
            readStream.push(null);
        }
    });
}


function queryAdditionalWeeks(name, count, generateEntireReport, res){
    const allparticipants = __dirname + `/final_week_${name}_week_batch.csv`;
    const outputpath = generateEntireReport ? allparticipants : __dirname + `/${name}_week_${Number(count)}_batch.csv`;
    const readStream = new stream.Readable({objectMode: true});
    readStream._read = () => {};
    readStream.pipe(writeToFile(outputpath, res, false)).on("finish", () => uploadToStorage(outputpath, res));
    openDatabase(`${name}_week_${Number(count)}_customer_details`,
    `${name}_week_${Number(count)}_migration`).then(client => {
        const unionCollections = [];
        for(let start=Number(count)-1;start >= 1; start--){
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
                ...unionCollections
        ];
        
       const report = client.collection.aggregate(pipeline);
       return readDatabaseCursor(report, outputpath, res, readStream).then(() => client.close());
    })
}

async function queryAllWeekParticipants(name, count, generateEntireReport, res){
    const allparticipants = __dirname + `/all_participants_${name}_week_batch.csv`;
    const outputpath = allparticipants;
    const readStream = new stream.Readable({objectMode: true});
    readStream._read = () => {};
    readStream.pipe(writeToFile(outputpath, res, false)).on("finish", () => uploadToStorage(outputpath, res));
    openDatabase(`${name}_week_${Number(count)}_customer_details`,
    `${name}_week_${Number(count)}_migration`).then(client => {
        const unionCollections = [];
        for(let start=Number(count)-1-1;start >= 1; start--){
            unionCollections.push(
                { "$unionWith": {
                    "coll": `${name}_week_${start}_customer_details`,
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
                ...unionCollections
        ];
        
       const report = client.collection.aggregate(pipeline);
       return readDatabaseCursor(report, outputpath, res, readStream).then(() => client.close());
    })
}


async function uploadToStorage(filePath, res){
    try{
        const bucket = firebase.storage().bucket(google_storage_bucket);
        const _storeInBucket = await bucket.upload(filePath);
        const getUrl = await _storeInBucket[0].getSignedUrl(expirydate);
        const url = getUrl[0];
        res.status(200).send({csvUrl: url});
    }catch(e){
        logger.info(e);
        res.status(500).send({message: "Failed to upload file to bucked" + e});
    }
}



module.exports= {generateReport, queryAllWeekParticipants};