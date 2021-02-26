const express = require('express');
const admin = require('firebase-admin');
const serviceAccount = require('./service-account.json');
const cors = require('cors');
const helmet = require('helmet');
const bodyParser = require('body-parser');
const rateLimit = require("express-rate-limit");
const port = 5000;
const add_customer_numbers = require('./controllers/add_customer_ids');
const reports = require('./controllers/reports');
const {getFirebaseUser} = require('./helpers/firebaseSecurity');

admin.initializeApp({
    credential: admin.credential.cert(serviceAccount),
});

const limiter = rateLimit({
    windowMs: 15 * 60 * 1000, // 15 minutes
    max: 100 // limit each IP to 100 requests per windowMs
});

const app = express();
app.use(cors());
app.use(helmet());
app.use(limiter);
app.use(bodyParser.json());
app.get('/refresh-token', getFirebaseUser, (req, res) => res.status(200).json({status: true, ...req.user}));
app.use('/api', getFirebaseUser, add_customer_numbers);
app.use('/api', getFirebaseUser, reports);
app.get('/', (req, res) => res.send({message: "App works"}));

app.listen(process.env.PORT || port, () => console.log('Running app 🤖🤖 ' + port));