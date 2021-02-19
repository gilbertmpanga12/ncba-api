const express = require('express');
const admin = require('firebase-admin');
const serviceAccount = require('./service-account.json');

const port = 5000;
const add_customer_numbers = require('./controllers/add_customer_ids');

admin.initializeApp({
    credential: admin.credential.cert(serviceAccount),
});

const app = express();
app.use('/api', add_customer_numbers);
app.get('/', (req, res) => res.send({message: "App works"}));
app.listen(port, () => console.log('Running app 🤖🤖 ' + port));