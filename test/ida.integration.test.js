const Environment = require("@superfluid-finance/ethereum-contracts/scripts/deploy-test-environment");
const IResolver = require("@superfluid-finance/ethereum-contracts/build/contracts/IResolver.json");
const ICFA = require("@superfluid-finance/ethereum-contracts/build/contracts/IConstantFlowAgreementV1.json");
const IIDA = require("@superfluid-finance/ethereum-contracts/build/contracts/IInstantDistributionAgreementV1.json");
const ISuperfluid = require("@superfluid-finance/ethereum-contracts/build/contracts/ISuperfluid.json");
const ISuperToken = require("@superfluid-finance/ethereum-contracts/build/contracts/ISuperToken.json");
const IToken = require("@superfluid-finance/ethereum-contracts/build/contracts/TestToken.json");

const expect = require('chai').expect
const Web3 = require('web3');
const traveler = require("ganache-time-traveler");
const ganache = require("../scripts/setGanache");
const App = require("../src/app");

const AGENT_ACCOUNT = "0x868D9F52f84d33261c03C8B77999f83501cF5A99";

let app, accounts, snapId, web3, ida, cfa, host, superToken, token, resolver, resolverAddress;

const delay = ms => new Promise(res => setTimeout(res, ms));
const exitWithError = (error) => {
    console.error(error);
    process.exit(1);
}
const setup = async () => {
    web3 = new Web3(ganache.provider);
    accounts = await web3.eth.getAccounts();
    await Environment((error) => {
            if(error)
                console.log(error);
        },{ web3: web3 }
    );
    resolverAddress = process.env.TEST_RESOLVER_ADDRESS;
    const superfluidIdent = `Superfluid.test`;
    resolver = new web3.eth.Contract(IResolver.abi,resolverAddress);
    const superfluidAddress = await resolver.methods.get(superfluidIdent).call();
    host = new web3.eth.Contract(ISuperfluid.abi,superfluidAddress);
    const cfaIdent = web3.utils.sha3("org.superfluid-finance.agreements.ConstantFlowAgreement.v1");
    const idaIdent = web3.utils.sha3("org.superfluid-finance.agreements.InstantDistributionAgreement.v1");
    const cfaAddress = await host.methods.getAgreementClass(cfaIdent).call();
    const idaAddress = await host.methods.getAgreementClass(idaIdent).call();
    cfa = new web3.eth.Contract(ICFA.abi, cfaAddress);
    ida = new web3.eth.Contract(IIDA.abi, idaAddress);
    const superTokenAddress = await resolver.methods.get("supertokens.test.fTUSDx").call();
    superToken = new web3.eth.Contract(ISuperToken.abi, superTokenAddress);
    const tokenAddress = await superToken.methods.getUnderlyingToken().call();
    const govAddress = await resolver.methods.get("TestGovernance.test").call();
    console.log("Governance Address ", govAddress);
    token = new web3.eth.Contract(IToken.abi, tokenAddress);

    for(const account of accounts) {
        await token.methods.mint(account,"10000000000000000000000").send({from: account});
        await token.methods.approve(superTokenAddress, "10000000000000000000000").send({from: account});
        await superToken.methods.upgrade("10000000000000000000000").send({from: account, gas:400000});
    }

    await web3.eth.sendTransaction({to:AGENT_ACCOUNT, from:accounts[9], value:web3.utils.toWei("10", "ether")})
};

const takeSnapshot = () => {
    return new Promise((resolve, reject) => {
      web3.currentProvider.send({
        jsonrpc: '2.0',
        method: 'evm_snapshot',
        id: new Date().getTime()
      }, (err, snapshotId) => {
        if (err) { return reject(err) }
        return resolve(snapshotId)
      })
    })
  }
const revertToSnapShot = (id) => {
    return new Promise((resolve, reject) => {
      web3.currentProvider.send({
        jsonrpc: '2.0',
        method: 'evm_revert',
        params: [id],
        id: new Date().getTime()
      }, (err, result) => {
        if (err) { return reject(err) }
        return resolve(result)
      })
    })
}

async function timeTravelOnce(time, setAppTime = false) {
    const block1 = await web3.eth.getBlock("latest");
    console.log("current block time", block1.timestamp);
    console.log(`time traveler going to the future +${time}...`);
    await traveler.advanceTimeAndBlock(time);
    const block2 = await web3.eth.getBlock("latest");
    console.log("new block time", block2.timestamp);
    if(setAppTime)
        app.setTime(block2.timestamp * 1000);
    return block2.timestamp;
}

const bootNode = async (delayParam = 0) => {
    app = new App({
        wsNode: "ws://127.0.0.1:8545",
        httpNode: "http://127.0.0.1:8545",
        mnemonic: "clutch mutual favorite scrap flag rifle tone brown forget verify galaxy return",
        epochBlock: 0,
        DB: "./mydatabase.sqlite",
        prv: "test",
        timeoutFn: 300000,
        pullStep: 500000,
        gasPrice:5000000000,
        concurrency: 1,
        coldBoot: 1,
        listenMode: 1,
        numberRetries: 3,
        testResolver: resolverAddress,
        liquidationDelay: delayParam,
        maxFee: 4000000000
    });
    app.start();
    while(!app.isInitialized()) {
        await delay(3000);
    }
}

const closeNode = async (force = false) => {
    if(app !== undefined)
        return app.shutdown(force);
}

const waitForEvent = async (eventName, blockNumber) => {
    await printEstimations();
    while(true) {
        try {
            const newBlockNumber = await web3.eth.getBlockNumber();
            console.log(`${blockNumber} - ${newBlockNumber}`);
            const events = await superToken.getPastEvents(eventName, {fromBlock: blockNumber, toBlock: newBlockNumber});
            if(events.length > 0) {
                return events;
            }
            await delay(1000);
            await timeTravelOnce(1, true);
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
describe("IDA integration tests", () => {

    before(async function() {
        await setup();
        snapId = await takeSnapshot();
        //await bootNode();
    });

    beforeEach(async () => {
        //console.log("Revert to snapshot with id: ", snapId.id);
        //await revertToSnapShot(snapId.id);
        await setup();
    });

   afterEach(async () => {
        //closeNode();
    });

    after(async () => {
        closeNode(true);
    });

    it("Get critical after IDA distribuiton", async () => {
        try {
            const cfaData = cfa.methods.createFlow(
                superToken._address,
                accounts[2],
                "1000000000000",
                "0x"
            ).encodeABI();
            await host.methods.callAgreement(cfa._address, cfaData, "0x").send({from: accounts[0], gas: 1000000});
            await timeTravelOnce(60);
            await bootNode();
            const data = ida.methods.createIndex(
                superToken._address, 6, "0x"
            ).encodeABI();
            await host.methods.callAgreement(ida._address, data, "0x").send({from: accounts[0], gas: 1000000});
            const subscriptionData = ida.methods.updateSubscription(
                superToken._address, 6, accounts[1], 100, "0x"
            ).encodeABI();
            await host.methods.callAgreement(ida._address, subscriptionData, "0x").send({from: accounts[0], gas: 1000000});
            const approveSubData = ida.methods.approveSubscription(
                    superToken._address,accounts[0],6,"0x"
                ).encodeABI();
            await host.methods.callAgreement(ida._address, approveSubData, "0x").send({from: accounts[1], gas: 1000000});
            await timeTravelOnce(60);
            const balance = await superToken.methods.realtimeBalanceOfNow(accounts[0]).call();
            const availableBalance = web3.utils.toBN(balance.availableBalance.toString());
            console.log(availableBalance.sub(web3.utils.toBN("100")));
            const distData =  ida.methods.distribute(
                superToken._address,
                6,
                availableBalance.sub(web3.utils.toBN("100")).toString(),
                "0x"
            ).encodeABI();
            const tx = await host.methods.callAgreement(ida._address, distData, "0x").send({from: accounts[0], gas: 1000000});
            const result = await waitForEvent("AgreementLiquidatedBy", tx.blockNumber);
            expectLiquidation(result[0], AGENT_ACCOUNT, accounts[0]);
        } catch(err) {
            exitWithError(err);
        }
    });
});