const stream = require('stream');

function storeData(dbCollection){
    const csvOutputStream = new stream.Writable({objectMode
    :true});
    console.log("I am running now ***");
    csvOutputStream._write = (chunk , encoding, callback) => {
        console.log(chunk );
        dbCollection.insertMany(chunk).then(() => {
            callback();
        }).catch((err) => {
            console.log('Error inserting',err);
            callback(err);
        });
    };

    return csvOutputStream;
}

module.exports = storeData;