const Sequelize = require('sequelize');
const db = require('../db');

const SystemModel = db.define("system", {
    blockNumber: {
        type: Sequelize.INTEGER,
        allowNull: false,
    },
    chainId: {
        type: Sequelize.INTEGER,
        allowNull: false,
    },
    superTokenBlockNumber: {
        type: Sequelize.INTEGER
    }
});

module.exports = SystemModel;