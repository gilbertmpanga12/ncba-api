const express = require('express');
const admin = require('firebase-admin');
const serviceAccount = require('./service-account.json');
const cors = require('cors');
const helmet = require('helmet');
const bodyParser = require('body-parser');
const rateLimit = require("express-rate-limit");
const port = 6000;
const add_customer_numbers = require('./controllers/add_customer_ids');
const reports = require('./controllers/reports');
const {getFirebaseUser} = require('./helpers/firebaseSecurity');
const writeToFile = require('./utilities/create_report');
const stream = require("stream");
const path = require('path');
const fs = require("fs");

admin.initializeApp({
    credential: admin.credential.cert(serviceAccount),
});

const limiter = rateLimit({
    windowMs: 15 * 60 * 1000, // 15 minutes
    max: 1000 // limit each IP to 100 requests per windowMs
});

const writeFile = fs.createWriteStream(path.join('./', `bitch_1.csv`));
writeToFile('bitch', 2).pipe(writeFile).on("data", function(data){
    console.log(data);
}).on("finish", function(){
    console.log("done!!!!!**");
});



const app = express();
app.use(cors());
app.use(helmet());
app.use(limiter);
app.use(bodyParser.json());
app.get('/ncba-api/refresh-token', getFirebaseUser, (req, res) => 
res.status(200).json({status: true, ...req.user}));
app.use('/ncba-api/api',getFirebaseUser,  add_customer_numbers); // ++ firebase user
app.use('/ncba-api/api/reports',getFirebaseUser, reports); // ++ firebase user
app.get('/ncba-api', (req, res) => res.send({message: "Raffle app active"}));


app.listen(port, () => console.log('Running app 🤖🤖 ' + port));