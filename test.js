const csv = require('csv-stream');
const request = require('request');
const url = `https://firebasestorage.googleapis.com/v0/b/wholesaleduuka-418f1.appspot.com/o/Test%20file%208.csv?alt=media&token=04024fa0-690b-4232-81ec-f820ce182141`;
const progress = require('request-progress');
// All of these arguments are optional.
const options = {
  
    columns : ['Customer Number', 'Loan Reference', 'Loan Start Date', 'Loan Repaid Date']
}

var counter = [];
var csvStream = csv.createStream(options);
progress(request(url)).on('progress', function (state) {
    console.log('progress', state)
}).pipe(csvStream).on('error',function(err){
        console.error(err);
    })
    .on('data',function(data){
        // outputs an object containing a set of key/value pair representing a line found in the csv file.
        counter.push(data['Customer Number']);
        // console.log(counter)
       
}).on('end',function(data){
    // outputs an object containing a set of key/value pair representing a line found in the csv file.
    // counter.push(data['Customer Number']);
    // console.log(counter)
    console.log('I have ended ***********************')
   
})