const csv = require('csv-stream');
const request = require('request');
//const url = `https://firebasestorage.googleapis.com/v0/b/wholesaleduuka-418f1.appspot.com/o/Test%20file%208.csv?alt=media&token=04024fa0-690b-4232-81ec-f820ce182141`;
const progress = require('request-progress');
// All of these arguments are optional.
const options = {
  
    columns : ['Customer Number', 'Loan Reference', 'Loan Start Date', 'Loan Repaid Date']
}

var counter = [];
// var csvStream = csv.createStream(options);
// progress(request(url)).on('progress', function (state) {
//     console.log('progress', state)
// }).pipe(csvStream).on('error',function(err){
//         console.error(err);
//     })
//     .on('data',function(data){
//         // outputs an object containing a set of key/value pair representing a line found in the csv file.
//         counter.push(data['Customer Number']);
//         // console.log(counter)
       
// }).on('end',function(data){
//     // outputs an object containing a set of key/value pair representing a line found in the csv file.
//     // counter.push(data['Customer Number']);
//     // console.log(counter)
//     console.log('I have ended ***********************')
   
// })

const url = 'https://firebasestorage.googleapis.com/v0/b/wholesaleduuka-418f1.appspot.com/o/NCBA%2Flite.csv?alt=media&token=a6eb526c-97ea-48e8-8480-f0fd060fc43c';
//const url = 'https://firebasestorage.googleapis.com/v0/b/wholesaleduuka-418f1.appspot.com/o/NCBA%2Ferror.csv?alt=media&token=f0251dc2-11d8-4f50-95d7-520aef6b858a'
const name = 'Holla';
const count = 8;
    console.log('PARMAS ****', {url, name, count})
    let datas = [];
    const csvStream = csv.createStream();
    progress(request(url)).on('progress', function (state) {
        console.log('progress', state);
    }).pipe(csvStream).on('error',function(err){
            console.error(err);
        })
        .on('data',function(csv_data){
            if(csv_data["Customer Number"] && 
            csv_data["Loan Reference"] && 
            csv_data["Loan Repaid Date"] 
            && csv_data["Loan Start Date"]){ // omit empty customer _ids
              datas.push({
                "Customer Number": csv_data["Customer Number"],
                "Loan Reference": csv_data["Loan Reference"],
                "Loan Repaid Date": csv_data["Loan Repaid Date"],
                "Loan Start Date": csv_data["Loan Start Date"],
              });
            }else{
              throw Error(`Please check your csv file for missing 
              blank customer numbers, or empty fields and 
              also ensure column names are correctly named`);
            }
           
    }).on('end',function(data){
       console.log(datas)
        


    })