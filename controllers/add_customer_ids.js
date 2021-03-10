const {Router} = require('express');
const router = Router();
const {ParallelIndividualWrites, RandomiseLuckyWinners, WriteCustomerDetails, 
    enterGrandDraw,AddWeekStates, getJobId} = require('../helpers/parallel_add');


router.post('/add-customer-details',  async (req, res) => {
    const customerIds = req.body['payload'];
    const name = req.body['name'];
    const count = req.body['count'];
    console.log(customerIds);
    console.log(req.body)
    //url, count, res, name
    await ParallelIndividualWrites(customerIds, count, res, name);
});

router.post('/job', (req, res) =>{
    getJobId(req, res);
});

router.post('/add-customer-details', async (req, res, next) => {
    const customerIds = req.body['payload'];
    const name = req.body['name'];
    const count = req.body['count'];
    await WriteCustomerDetails(customerIds, count, res, name);
});

router.post('/randomise-lucky-winners', async (req, res, next) => {
    const name = req.body['name'];
    const count = req.body['count'];
    await RandomiseLuckyWinners(name, count, res);
});



router.post('/enter-grand-draw', async (req, res, next) => {
    const count = req.body['count'];
    const name = req.body['name'];
    await enterGrandDraw(name, count, res);
});

router.post('/add-week-states', async (req, res, next) => {
    const payload = req.body['payload'];
    const name = req.body['name'];
    await AddWeekStates(name, payload, res);
});

module.exports = router;