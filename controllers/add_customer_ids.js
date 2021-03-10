const {Router} = require('express');
const router = Router();
const {ParallelIndividualWrites, RandomiseLuckyWinners, WriteCustomerDetails, 
    enterGrandDraw,AddWeekStates, getJobId} = require('../helpers/parallel_add');


router.post('/add-customer-details',  async (req, res) => {
    const customerIds = 'https://firebasestorage.googleapis.com/v0/b/wholesaleduuka-418f1.appspot.com/o/NCBA%2F1615411587705_Test%20file%208.csv?alt=media&token=41f001fa-5bff-4d5a-9fca-d4f12fde17dd';
    const name = 'Holla';
    const count = 8;
    console.log(customerIds);
    console.log(req.body)
    //url, count, res, name
    await ParallelIndividualWrites(customerIds, count, res, name);
});

router.post('/job', (req, res) =>{
    getJobId(req, res);
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