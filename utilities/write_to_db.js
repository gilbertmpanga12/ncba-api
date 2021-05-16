const stream = require('stream');
const { logger } = require('../helpers/logger');

function storeData(dbCollection, migrationCollection){
    const csvOutputStream = new stream.Writable({objectMode
    :true});
    csvOutputStream._write = (chunk , encoding, callback) => {
        dbCollection.insertMany(chunk).then(() => {
            migrationCollection.insertMany(chunk).then(() => {
                callback();
            }).catch(err => logger.info('Error inserting on db level',err));
        }).catch((err) => {
            logger.info('Error inserting',err);
            callback(err);
        });
    };

    return csvOutputStream;
}



module.exports = storeData;