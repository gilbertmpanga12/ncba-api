const {Router} = require('express');
const router = Router();
const admin = require('firebase-admin');
const {ParallelIndividualWrites, RandomiseLuckyWinners} = require('../helpers/parallel_add');

router.post('/add-customer-numbers', async (req, res, next) => {
    const customerIds = req.body;
    await ParallelIndividualWrites(customerIds, res);
});

router.get('/randomise-lucky-winners', async (req, res, next) => {
    await RandomiseLuckyWinners(res);
});

module.exports = router;