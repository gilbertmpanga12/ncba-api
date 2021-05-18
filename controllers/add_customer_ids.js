const {Router} = require('express');
const router = Router();
const {ParallelIndividualWrites, RandomiseLuckyWinners,
    AddWeekStates, getJobId, pickLucky3, currentWeek, validateCsvFile} = require('../helpers/parallel_add');



router.post('/add-customer-details',  async (req, res) => {
    const {url, name, count, operation, weekDuration} = req.body;
    ParallelIndividualWrites(url, count, res, name, operation, weekDuration);
});

router.post('/job', (req, res) =>{
    getJobId(req, res);
});


router.post('/randomise-lucky-winners', async (req, res, next) => {
    const {weekDuration, name, count} = req.body;
    await RandomiseLuckyWinners(name, count, weekDuration, res);
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

router.post('/pick-lucky-3', async (req, res, next) => {
    const {name} = req.body;
    pickLucky3(name, res);
});



router.post('/current-week', (req, res) => {
    const {count, name, page, docsCount} = req.body;
    currentWeek(count, name, docsCount, page, res);
});


router.post('/validate-csv-file', async (req, res) => {
    const {url} = req.body;
    req.setTimeout(5000000000);
    await validateCsvFile(url);
});

module.exports = router;