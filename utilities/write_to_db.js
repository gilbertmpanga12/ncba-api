const stream = require('stream');
const { logger } = require('../helpers/logger');
const openDatabase = require('./mongo_client');

function storeData(dbCollection){
    const csvOutputStream = new stream.Writable({objectMode
    :true});
    csvOutputStream._write = (chunk , encoding, callback) => {
        dbCollection.insertMany(chunk).then(() => {
            callback();
        }).catch((err) => {
            logger.info('Error inserting',err);
            callback(err);
        });
    };

    return csvOutputStream;
}

function storeMigrationData(name, count){
    openDatabase(`${name}_week_${count}_migration`).then(client => {
        const csvOutputStream = new stream.Writable({objectMode
            :true});
            csvOutputStream._write = (chunk , encoding, callback) => {
                client.collection.insertMany.insertMany(chunk).then(() => {
                    callback();
                }).catch((err) => {
                    logger.info('Error inserting',err);
                    callback(err);
                });
            };
        
            return csvOutputStream;
    }).catch(err => logger.info(err));
    
}

module.exports = {storeData, storeMigrationData};