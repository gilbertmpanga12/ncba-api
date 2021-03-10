const csv = require('csv-stream');
const request = require('request');
 const url = `https://firebasestorage.googleapis.com/v0/b/wholesaleduuka-418f1.appspot.com/o/Test%20file%208.csv?alt=media&token=04024fa0-690b-4232-81ec-f820ce182141`;

// All of these arguments are optional.
var options = {
  
    columns : ['Customer Number', 'Loan Reference', 'Loan Start Date', 'Loan Repaid Date']
}
 
var csvStream = csv.createStream(options);
request(url).pipe(csvStream)
    .on('error',function(err){
        console.error(err);
    })
    .on('data',function(data){
        // outputs an object containing a set of key/value pair representing a line found in the csv file.
        console.log({customerId:data['Customer Number']});
});
