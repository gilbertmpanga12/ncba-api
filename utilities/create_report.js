const stream = require('stream');
const { firestore } = require('firebase-admin');
const papaparse = require('papaparse');

function writeToFile(name, count){
    let firstOutput = true;
    const readStream = new stream.Readable({objectMode: true});
    readStream._read = () => {};
    firestore().collection(`${name}_week_${count}_customer_details`).get().then(data => {
        data.forEach(customer_data => {
          
            const outputCsv = papaparse.unparse([{
				"Customer Number": customer_data.data()["Customer Number"],
				"Loan Reference": customer_data.data()["Loan Reference"],
				"Loan Repaid Date": customer_data.data()["Loan Repaid Date"],
				"Loan Start Date": customer_data.data()["Loan Start Date"]
			  }], {
                header:  firstOutput
            });
            readStream.push(outputCsv + "\n");
            firstOutput = false;
        });
       readStream.push(null);
    }).catch(err => {
        readStream.push(null);
    });

    return readStream;
}


module.exports = writeToFile;
