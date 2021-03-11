const csv = require('csv-stream');
const request = require('request');
const url = `https://firebasestorage.googleapis.com/v0/b/wholesaleduuka-418f1.appspot.com/o/Test%20file%208.csv?alt=media&token=04024fa0-690b-4232-81ec-f820ce182141`;
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

//const url = 'https://firebasestorage.googleapis.com/v0/b/wholesaleduuka-418f1.appspot.com/o/NCBA%2Flite.csv?alt=media&token=a6eb526c-97ea-48e8-8480-f0fd060fc43c';
const name = 'Holla';
const count = 8;
    console.log('PARMAS ****', {url, name, count})
    let payload = [];
    const csvStream = csv.createStream(options);
    progress(request(url)).on('progress', function (state) {
        console.log('progress', state);
    }).pipe(csvStream).on('error',function(err){
            console.error(err);
        })
        .on('data',function(data){
           payload.push({
        'Customer Number': data['Customer Number'],
        'Loan Reference': data['Loan Reference'],
        'Loan Repaid date': data['Loan Repaid Date'],
        'Loan Start Date': data['Loan Start Date']
    });
           
    }).on('end',function(data){
       
        let progress_details = 0;
        let progress_points = 0;
        // USER POINTS
        const user_points = payload;
        
        let user_points_length = user_points.length;
        let counter_500s = 0;
        // let customerPoints = firestore().batch();
        let remainder = user_points_length;
        for(var start=1; start <= user_points_length; start++){
            counter_500s += 1;
            // if(remainder < 500){
            //     // console.log({customerId: user_points[start]['Customer Number'], 
            //     // loanReference: user_points[start]['Loan Reference']})
            //     // final increments
            //     progress_points = 50;
            //     // job.progress(progress_points);
            //     break;
            // }

            console.log({customerId: user_points[start]['Customer Number'], 
            loanReference: user_points[start]['Loan Reference']})
            if(counter_500s === 500){
               
                counter_500s = 0;
                remainder -= 500;
                // minor increments
                progress_points += 1;
                // job.progress(progress_points);
                continue;
            }
        }

        // // USER DETAILS
        // const user_details = payload;
        // let user_details_length = user_details.length;
        // let counter_points_500s = 0;
      
        // let remainder_details = user_details_length;
        // for(var start=1; start <= user_details_length; start++){
        //     counter_points_500s += 1;
        //     // if(remainder_details < 500){
        //     //     console.log('index', start)
        //     //     console.log( {customerNumber:user_details[remainder_details]});
        //     //     // let remainder_count =remainder_details;
        //     //     // while(remainder_details < 500){
        //     //     //     console.log( {customerNumber:user_details[remainder_details]['Customer Number']});
        //     //     //     remainder_details++;
        //     //     //     progress_details = 50;
                   
        //     //     // }
        //     //     progress_details = 50;
        //     //     break;
               
               
        //     // }
        //    console.log({...user_details[start]});
        //     if(counter_points_500s === 500){
        //        console.log('am called honey bee')
        //         counter_points_500s = 0;
        //         remainder_details -= 500;
        //         // minor increments
        //         progress_details += 1;
        //         // job.progress(progress_details);
        //         continue;
        //     }
            
        //    }

        //   var final_progress = progress_details + progress_points;
        //   if(final_progress === 100){
        //       console.log('dones progress')
        //     // job.progress(final_progress);
        //   }

    })