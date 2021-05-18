const stream = require('stream');
const moment = require('moment');

function validateJSONData(){
    const transformStream = new stream.Transform({objectMode: true});
    transformStream._transform = (chunk, encoding, callback) => {
    let data = chunk;
    data['Loan Repaid Date'] = moment(data["Loan Repaid Date"]).format();
    data['Loan Start Date'] = moment(data["Loan Start Date"]).format();
    transformStream.push(data);
    callback();
    }
    return transformStream;
}


module.exports = validateJSONData;