const { logger } = require('../helpers/logger');
const openDatabase = require('../utilities/mongo_client');
const stream = require('stream');
const writeToFile = require('../utilities/create_report');
const path = require('path');


function generateReport(name,count, res){
    if(Number(count) > 1){
        queryAdditionalWeeks(name, count)
        return;
    }
    queryFirstWeek(name, count, res);
}


function queryFirstWeek(name, count, res){
    const outputpath = __dirname + `/${name}_week_${count}_batch.csv`;
    const readStream = new stream.Readable({objectMode: true});
    readStream._read = () => {};
    readStream.pipe(writeToFile(outputpath, res, false)).on("finish", () => console.log("bitch am done"));
    openDatabase(`${name}_week_${count}_customer_details`).then(client => {
        const cursor = client.collection.find({});
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


function queryAdditionalWeeks(name, count, res){
    const outputpath = path.join('./', 'weeks/' + `${name}_week_${count}_batch.csv`);
    const readStream = new stream.Readable({objectMode: true});
    readStream._read = () => {};
    readStream.pipe(writeToFile(outputpath, res, false)).on("finish", () => console.log("bitch am done"));
    openDatabase(`${name}_week_${count}_customer_details`).then(client => {
        const pipeline = [
            { "$project": { "Customer Number": true, "Loan Reference": true , "Loan Repaid Date": true, "Loan Start Date": true} },
            { "$unionWith": `${name}_week_${count - 1}_customer_details` }
        ];
       const report = client.collection.aggregate(pipeline);
       return readDatabaseCursor(report, outputpath, res, readStream).then(() => client.close());
    })
}




module.exports= generateReport;