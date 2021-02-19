const {Router} = require('express');
const router = Router();
const {ParallelIndividualWrites, RandomiseLuckyWinners} = require('../helpers/parallel_add');

router.post('/add-customer-numbers', async (req, res, next) => {
    const customerIds = req.body;
    console.log(customerIds)
    await ParallelIndividualWrites(customerIds, res);
});

router.get('/randomise-lucky-winners', async (req, res, next) => {
    await RandomiseLuckyWinners(res);
});

module.exports = router;