const protocolHelper = require("../../test/utils/protocolHelper");
const expect = require("chai").expect;
const startGanache = require("../../test/utils/ganache");
const App = require("../../src/app");

const AGENT_ACCOUNT = "0x868D9F52f84d33261c03C8B77999f83501cF5A99";
const DEFAULT_REWARD_ADDRESS = "0x0000000000000000000000000000000000000045";

let app, accounts, snapId, helper, web3, ganache, provider;

const bootNode = async (config) => {
  const sentinelConfig = protocolHelper.getSentinelConfig(config);
  app = new App(sentinelConfig);
  app.start();
  while (!app.isInitialized()) {
    await protocolHelper.timeout(5000);
  }
};

const closeNode = async (force = false) => {
  if (app !== undefined) {
    return app.shutdown(force);
  }
};

describe("CFA tests", () => {

  before(async function () {
    ganache = await startGanache();
    provider = await ganache.provider;
    helper = await protocolHelper.setup(provider, AGENT_ACCOUNT);
    helper.provider = provider;
    web3 = helper.web3;
    accounts = helper.accounts;
    snapId = await ganache.helper.takeEvmSnapshot(provider);
  });

  beforeEach(async () => {
  });

  afterEach(async () => {
    try {
      await ganache.helper.revertToSnapShot(provider, snapId);
    } catch (err) {
      protocolHelper.exitWithError(err);
    }
  });

  after(async () => {
    if(!app._isShutdown) {
      await closeNode(true);
    }
    await ganache.close();
  });

  it.only("Create one stream", async () => {
    try {
      await helper.operations.createStream(helper.sf.superToken.options.address, accounts[0], accounts[2], "100000000000");
      await ganache.helper.timeTravelOnce(provider, web3,1);
      await bootNode({pic: DEFAULT_REWARD_ADDRESS, resolver: helper.sf.resolver.options.address, log_level: "debug"});
      await ganache.helper.timeTravelOnce(provider, web3, 60);
      const tx = await helper.sf.superToken.methods.transferAll(accounts[2]).send({
        from: accounts[0],
        gas: 1000000
      });
      const result = await protocolHelper.waitForEvent(helper, app, ganache, "AgreementLiquidatedV2", tx.blockNumber);
      await app.shutdown();
      protocolHelper.expectLiquidationV2(result[0], AGENT_ACCOUNT, accounts[0], "0");
    } catch (err) {
      protocolHelper.exitWithError(err);
    }
  });

  it("Create small stream then updated to bigger stream", async () => {
    try {
      await helper.operations.createStream(helper.sf.superToken.options.address, accounts[0], accounts[2], "1000");
      await ganache.helper.timeTravelOnce(provider, web3,1);
      await bootNode({pic: accounts[0]});
      await ganache.helper.timeTravelOnce(provider, web3, 60);
      await helper.operations.updateStream(helper.sf.superToken.options.address, accounts[0], accounts[2], "100000000000");
      await ganache.helper.timeTravelOnce(provider, web3, 60);
      const tx = await helper.sf.superToken.methods.transferAll(accounts[2]).send({
        from: accounts[0],
        gas: 1000000
      });
      const result = await protocolHelper.waitForEvent(helper, app, ganache, "AgreementLiquidatedV2", tx.blockNumber);
      await app.shutdown();
      protocolHelper.expectLiquidationV2(result[0], AGENT_ACCOUNT, accounts[0], "0");
    } catch (err) {
      protocolHelper.exitWithError(err);
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
        "0x").send({
        from: accounts[0],
        gas: 1000000
      });
      await ganache.helper.timeTravelOnce(1);
      await bootNode({pic: accounts[0]});
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
        "0x").send({
        from: accounts[2],
        gas: 1000000
      });
      await ganache.helper.timeTravelOnce(60);
      const tx = await protocolVars.superToken.methods.transferAll(accounts[5]).send({
        from: accounts[0],
        gas: 1000000
      });
      const result = await protocolHelper.waitForEvent(protocolVars, app, ganache, "AgreementLiquidatedV2", tx.blockNumber);
      await app.shutdown();
      protocolHelper.expectLiquidationV2(result[0], AGENT_ACCOUNT, accounts[0], "0");
    } catch (err) {
      protocolHelper.exitWithError(err);
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
      await protocolVars.host.methods.callAgreement(protocolVars.cfa._address, flowData, "0x").send({
        from: accounts[0],
        gas: 1000000
      });
      await ganache.helper.timeTravelOnce(1);
      await bootNode({pic: accounts[0]});
      await ganache.helper.timeTravelOnce(3600, app, true);
      const flowData2 = protocolVars.cfa.methods.createFlow(
        protocolVars.superToken._address,
        accounts[3],
        "1000000000000000",
        "0x"
      ).encodeABI();
      await protocolVars.host.methods.callAgreement(protocolVars.cfa._address, flowData2, "0x").send({
        from: accounts[0],
        gas: 1000000
      });
      await protocolVars.superToken.methods.transferAll(accounts[9]).send({
        from: accounts[0],
        gas: 1000000
      });
      const result = await protocolHelper.waitForEvent(protocolVars, app, ganache, "AgreementLiquidatedV2", 0);
      await app.shutdown();
      protocolHelper.expectLiquidationV2(result[0], AGENT_ACCOUNT, accounts[0], "0");

    } catch (err) {
      protocolHelper.exitWithError(err);
    }
  });

  it("Create a stream with big flow rate, then update the stream with smaller flow rate", async () => {
    try {
      const flowData = protocolVars.cfa.methods.createFlow(
        protocolVars.superToken._address,
        accounts[2],
        "100000000000000",
        "0x"
      ).encodeABI();
      await protocolVars.host.methods.callAgreement(protocolVars.cfa._address, flowData, "0x").send({
        from: accounts[5],
        gas: 1000000
      });
      await ganache.helper.timeTravelOnce(1);
      await bootNode();
      const firstEstimation = await app.db.queries.getAddressEstimations(accounts[5]);
      const updateData = protocolVars.cfa.methods.updateFlow(
        protocolVars.superToken._address,
        accounts[2],
        "1",
        "0x"
      ).encodeABI();
      await ganache.helper.timeTravelUntil(1, 20);
      await protocolVars.host.methods.callAgreement(protocolVars.cfa._address, updateData, "0x").send({
        from: accounts[5],
        gas: 1000000
      });
      await ganache.helper.timeTravelUntil(1, 20);
      const secondEstimation = await app.db.queries.getAddressEstimations(accounts[5]);
      await app.shutdown();
      console.log("Estimation 1: ", firstEstimation[0].estimation);
      console.log("Estimation 2: ", secondEstimation[0].estimation);
      expect(firstEstimation[0].estimation).to.not.equal(32503593600000);
      // the stream is soo small that we mark as not a real estimation
      expect(secondEstimation[0].estimation).to.equal(32503593600000);
    } catch (err) {
      protocolHelper.exitWithError(err);
    }
  });

  it("Should make liquidation as Pleb", async() => {
    try {
      const data = protocolVars.cfa.methods.createFlow(
          protocolVars.superToken._address,
          accounts[2],
          "10000000000000000",
          "0x"
      ).encodeABI();
      await protocolVars.host.methods.callAgreement(protocolVars.cfa._address, data, "0x").send({
        from: accounts[0],
        gas: 1000000
      });
      await ganache.helper.timeTravelOnce(1);
      await bootNode({log_level:"debug"});
      await ganache.helper.timeTravelOnce(60);
      const tx = await protocolVars.superToken.methods.transferAll(accounts[2]).send({
        from: accounts[0],
        gas: 1000000
      });
      await ganache.helper.timeTravelOnce(900);
      const result = await protocolHelper.waitForEvent(protocolVars, app, ganache, "AgreementLiquidatedV2", tx.blockNumber);
      await app.shutdown();
      protocolHelper.expectLiquidationV2(result[0], AGENT_ACCOUNT, accounts[0], "1");
    } catch (err) {
      protocolHelper.exitWithError(err);
    }
  });

  it("Should make liquidation wait until Pleb slot", async() => {
    try {
      const data = protocolVars.cfa.methods.createFlow(
          protocolVars.superToken._address,
          accounts[2],
          "10000000000000000",
          "0x"
      ).encodeABI();
      await protocolVars.host.methods.callAgreement(protocolVars.cfa._address, data, "0x").send({
        from: accounts[0],
        gas: 1000000
      });
      await ganache.helper.timeTravelOnce(1);
      await bootNode({log_level:"debug"});
      await ganache.helper.timeTravelOnce(60);
      const tx = await protocolVars.superToken.methods.transferAll(accounts[2]).send({
        from: accounts[0],
        gas: 1000000
      });
      await ganache.helper.timeTravelOnce(850);
      const result = await protocolHelper.waitForEvent(protocolVars, app, ganache, "AgreementLiquidatedV2", tx.blockNumber);
      await app.shutdown();
      protocolHelper.expectLiquidationV2(result[0], AGENT_ACCOUNT, accounts[0], "1");
    } catch (err) {
      protocolHelper.exitWithError(err);
    }
  });

  it("Should make liquidation as Pirate", async() => {
    try {
      const data = protocolVars.cfa.methods.createFlow(
          protocolVars.superToken._address,
          accounts[2],
          "10000000000000000",
          "0x"
      ).encodeABI();
      await protocolVars.host.methods.callAgreement(protocolVars.cfa._address, data, "0x").send({
        from: accounts[0],
        gas: 1000000
      });
      await ganache.helper.timeTravelOnce(1);
      await bootNode({pirate: "true"});
      await ganache.helper.timeTravelOnce(60);
      const tx = await protocolVars.superToken.methods.transferAll(accounts[2]).send({
        from: accounts[0],
        gas: 1000000
      });
      await ganache.helper.timeTravelOnce(14400);
      const result = await protocolHelper.waitForEvent(protocolVars, app, ganache, "AgreementLiquidatedV2", tx.blockNumber);
      await app.shutdown();
      protocolHelper.expectLiquidationV2(result[0], AGENT_ACCOUNT, accounts[0], "2");
    } catch (err) {
      protocolHelper.exitWithError(err);
    }
  });

  it("Subscribe to token runtime", async () => {
    try {
      await bootNode({pic: accounts[0]});
      const data = protocolVars.cfa.methods.createFlow(
          protocolVars.superToken._address,
          accounts[2],
          "10000000000000000",
          "0x"
      ).encodeABI();
      await protocolVars.host.methods.callAgreement(protocolVars.cfa._address, data, "0x").send({
        from: accounts[0],
        gas: 1000000
      });
      //await ganache.helper.timeTravelOnce(60);
      const tx = await protocolVars.superToken.methods.transferAll(accounts[2]).send({
        from: accounts[0],
        gas: 1000000
      });
      const result = await protocolHelper.waitForEvent(protocolVars, app, ganache, "AgreementLiquidatedV2", tx.blockNumber);
      await app.shutdown();
      protocolHelper.expectLiquidationV2(result[0], AGENT_ACCOUNT, accounts[0], "0");
    } catch (err) {
      protocolHelper.exitWithError(err);
    }
  });
});
