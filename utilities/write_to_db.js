const stream = require('stream');

function storeData(dbCollection){
    const csvOutputStream = new stream.Writable({objectMode
    :true});
    csvOutputStream._write = (chunk , encoding, callback) => {
        dbCollection.insertMany(chunk).then(() => {
            callback();
        }).catch((err) => {
            callback(err);
        });
    };

    return csvOutputStream;
}

module.exports = storeData;