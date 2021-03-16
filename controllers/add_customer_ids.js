const {Router} = require('express');
const router = Router();
const {ParallelIndividualWrites, RandomiseLuckyWinners,
    enterGrandDraw,AddWeekStates, getJobId} = require('../helpers/parallel_add');


router.post('/add-customer-details',  async (req, res) => {
    const {url, name, count, operation} = req.body;
    ParallelIndividualWrites(url, count, res, name, operation);
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