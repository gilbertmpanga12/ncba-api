const { logger } = require('../helpers/logger');
const openDatabase = require('../utilities/mongo_client');

function generateReport(name,count, res){
    if(Number(count) > 1){
        queryAdditionalWeeks(name, count)
        return;
    }
    queryFirstWeek(name, count);
}


function queryFirstWeek(name, count){
    openDatabase(`${name}_week_${count}_customer_details`).then(client => {
        const cursor = client.collection.find({});
        return readDatabaseCursor(cursor).then(() => client.close());
    }).catch(err => {
        logger.info(err);
    });
}

// readdatabse cursor for first week
function readDatabaseCursor(cursor){
    return cursor.next().then(record => {
        if(record){
            // do something here;
            return readDatabaseCursor(cursor);
        }else{
            // nothing to process;
        }
    });
}


function queryAdditionalWeeks(name, count){
    openDatabase(`${name}_week_${count}_customer_details`).then(client => {
        const pipeline = [
            { "$project": { "Customer Number": true, "Loan Reference": true , "Loan Repaid Date": true, "Loan Start Date": true} },
            { "$unionWith": `${name}_week_${count - 1}_customer_details` }
        ];
       const report = client.collection.aggregate(pipeline);
       return readDatabaseCursor(report).then(() => client.close());
    })
}


module.exports= generateReport;