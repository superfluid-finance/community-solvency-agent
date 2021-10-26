const protocolHelper = require("../utils/protocolHelper");
const expect = require("chai").expect
const ganache = require("../utils/ganache");
const App = require("../../src/app");

const AGENT_ACCOUNT = "0x868D9F52f84d33261c03C8B77999f83501cF5A99";

let app, accounts, snapId, protocolVars, web3;

const delay = ms => new Promise(res => setTimeout(res, ms));
const exitWithError = (error) => {
    console.error(error);
    process.exit(1);
}

const bootNode = async (delayParam = 0) => {
    app = new App({
        http_rpc_node: "http://127.0.0.1:8545",
        mnemonic: "clutch mutual favorite scrap flag rifle tone brown forget verify galaxy return",
        mnemonic_index: 100,
        epoch_block: 0,
        DB: "datadir/test.sqlite",
        protocol_release_version: "test",
        tx_timeout: 30,
        max_query_block_range: 500000,
        max_gas_price:4000000000,
        concurrency: 1,
        cold_boot: 1,
        only_listed_tokens: 1,
        number_retries: 3,
        test_resolver: resolverAddress,
        additional_liquidation_delay: delayParam,
        liquidation_run_every: 5000,
        polling_interval: 10
    });
    app.start();
    while(!app.isInitialized()) {
        await delay(3000);
    }
}

const stopSentinel = async (force = false) => {
    if(app !== undefined)
        return app.shutdown(force);
}

const waitForEvent = async (eventName, blockNumber) => {
    await printEstimations();
    while(true) {
        try {
            const newBlockNumber = await web3.eth.getBlockNumber();
            console.log(`${blockNumber} - ${newBlockNumber}`);
            const events = await protocolVars.superToken.getPastEvents(eventName, {fromBlock: blockNumber, toBlock: newBlockNumber});
            if(events.length > 0) {
                return events;
            }
            await delay(1000);
            await ganache.helper.timeTravelOnce(1, app, true);
        } catch(err) {
            exitWithError(err);
        }
    }
}

const printEstimations = async () => {
    console.log("==========ESTIMATIONS==========");
    const estimations = await app.getEstimations();
    for(const est of estimations) {
        console.log(`SuperToken: ${est.superToken} - account: ${est.address} : ${new Date(est.zestimation) }`);
    }
    console.log("===============================");
}

const expectLiquidation = (event, node, account) => {
    expect(event.returnValues.liquidatorAccount).to.equal(node);
    expect(event.returnValues.bailoutAmount).to.equal("0");
    expect(event.returnValues.penaltyAccount).to.equal(account);
}

const expectBailout = (event, node, account) => {
    expect(event.returnValues.liquidatorAccount).to.equal(node);
    expect(event.returnValues.bailoutAmount).not.equal("0");
    expect(event.returnValues.penaltyAccount).to.equal(account);
}

describe("Integration scripts tests", () => {

    before(async function() {
        protocolVars = await protocolHelper.setup(ganache.provider, AGENT_ACCOUNT);
        web3 = protocolVars.web3;
        accounts = protocolVars.accounts;
        snapId = await ganache.helper.takeEvmSnapshot();
    });

    beforeEach(async () => {
    });

   afterEach(async () => {
        try {
            const result = await stopSentinel();
            console.log("HERE")
            console.log(result);
            snapId = await ganache.helper.revertToSnapShot(snapId.result);
        } catch(err) {
            exitWithError(err);
        }
    });

    after(async () => {
        //await stopSentinel();
        //console.log(ganache.close);
        ganache.close();
    });

    it.only("Create one stream", async () => {
        try {
            const data = protocolVars.cfa.methods.createFlow(
                protocolVars.superToken._address,
                accounts[2],
                "10000000000000000",
                "0x"
            ).encodeABI();
            await protocolVars.host.methods.callAgreement(protocolVars.cfa._address, data, "0x").send({from: accounts[0], gas: 1000000});
            await bootNode();
            const tx = await protocolVars.superToken.methods.transferAll(accounts[2]).send({from: accounts[0], gas: 1000000});
            const result = await waitForEvent("AgreementLiquidatedBy", tx.blockNumber);
            expectLiquidation(result[0], AGENT_ACCOUNT, accounts[0]);
        } catch(err) {
            exitWithError(err);
        }
    });

    it("Create small stream then updated to bigger stream", async () => {
        try {
            const data = protocolVars.cfa.methods.createFlow(
                protocolVars.superToken._address,
                accounts[2],
                "1000",
                "0x"
            ).encodeABI();
            await protocolVars.host.methods.callAgreement(protocolVars.cfa._address, data, "0x").send({from: accounts[0], gas: 1000000});
            await bootNode();
            await ganache.helper.timeTravelOnce(60);
            const dataUpdate = protocolVars.cfa.methods.updateFlow(
                protocolVars.superToken._address,
                accounts[2],
                "1000000000",
                "0x"
            ).encodeABI();
            await protocolVars.host.methods.callAgreement(protocolVars.cfa._address, dataUpdate, "0x").send({from: accounts[0], gas: 1000000});
            await ganache.helper.timeTravelOnce(60);
            const tx = await protocolVars.superToken.methods.transferAll(accounts[2]).send({from: accounts[0], gas: 1000000});
            const result = await waitForEvent("AgreementLiquidatedBy", tx.blockNumber);
            expectLiquidation(result[0], AGENT_ACCOUNT, accounts[0]);
        } catch(err) {
            exitWithError(err);
        }
    });

    it("Create one out going stream and receive a smaller incoming stream", async () => {
        try {
            const sendingFlowData = protocolVars.cfa.methods.createFlow(
                protocolVars.superToken._address,
                accounts[2],
                "1000000000",
                "0x"
            ).encodeABI();
            await protocolVars.host.methods.callAgreement(
                protocolVars.cfa._address,
                sendingFlowData,
                "0x").send({from: accounts[0], gas: 1000000});
            await bootNode();
            await ganache.helper.timeTravelOnce(60);
            const receivingFlowData = protocolVars.cfa.methods.createFlow(
                protocolVars.superToken._address,
                accounts[0],
                "10000",
                "0x"
            ).encodeABI();
            await protocolVars.host.methods.callAgreement(
                protocolVars.cfa._address,
                receivingFlowData,
                "0x").send({from: accounts[2], gas: 1000000});
            await ganache.helper.timeTravelOnce(60);
            const tx = await protocolVars.superToken.methods.transferAll(accounts[5]).send({from: accounts[0], gas: 1000000});
            const result = await waitForEvent("AgreementLiquidatedBy", tx.blockNumber);
            expectLiquidation(result[0], AGENT_ACCOUNT, accounts[0]);
        } catch(err) {
            exitWithError(err);
        }
    });

    it("Create two outgoing streams, and new total outflow rate should apply to the agent estimation logic", async () => {
        try {
            const flowData = protocolVars.cfa.methods.createFlow(
                protocolVars.superToken._address,
                accounts[2],
                "1000000000000000",
                "0x"
            ).encodeABI();
            await protocolVars.host.methods.callAgreement(protocolVars.cfa._address, flowData, "0x").send({from: accounts[0], gas: 1000000});
            await bootNode();
            await ganache.helper.timeTravelOnce(3600, app, true);
            const flowData2 = protocolVars.cfa.methods.createFlow(
                protocolVars.superToken._address,
                accounts[3],
                "1000000000000000",
                "0x"
            ).encodeABI();
            await protocolVars.host.methods.callAgreement(protocolVars.cfa._address, flowData2, "0x").send({from: accounts[0], gas: 1000000});
            await protocolVars.superToken.methods.transferAll(accounts[9]).send({from: accounts[0], gas: 1000000});
            //await timeTravelOnce(3600, true);
            let result = await waitForEvent("AgreementLiquidatedBy", 0);
            expectLiquidation(result[0], AGENT_ACCOUNT, accounts[0]);
        } catch(err) {
            exitWithError(err);
        }
    });

    it.skip("Create a stream with big flow rate, then update the stream with smaller flow rate", async () => {
        try {
            const flowData = protocolVars.cfa.methods.createFlow(
                protocolVars.superToken._address,
                accounts[2],
                "100000000000000",
                "0x"
            ).encodeABI();
            await protocolVars.host.methods.callAgreement(protocolVars.cfa._address, flowData, "0x").send({from: accounts[5], gas: 1000000});
            await ganache.helper.timeTravelOnce(60);
            await bootNode();
            //await ganache.helper.timeTravelOnce(60);
            const firstEstimation = await app.db.queries.getAddressEstimation(accounts[5]);
            //await ganache.helper.timeTravelUntil(1, 20);
            const updateData = protocolVars.cfa.methods.updateFlow(
                protocolVars.superToken._address,
                accounts[2],
                "1",
                "0x"
            ).encodeABI();
            await ganache.helper.timeTravelUntil(1, 20);
            const x = await protocolVars.host.methods.callAgreement(protocolVars.cfa._address, updateData, "0x").send({from: accounts[5], gas: 1000000});
            console.log(x)
            await ganache.helper.timeTravelUntil(1, 20);
            const secondEstimation = await app.db.queries.getAddressEstimation(accounts[5]);
            console.log("Estimation 1: ", firstEstimation[0].zestimation)
            console.log("Estimation 2: ", secondEstimation[0].zestimation)
            expect(firstEstimation[0].zestimation).to.not.equal(32503593600000);
            //the stream is soo small that we mark as not a real estimation
            expect(secondEstimation[0].zestimation).to.equal(32503593600000);
        } catch(err) {
            exitWithError(err);
        }
    });
});