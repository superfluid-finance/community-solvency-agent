const async = require("async");

// rename to EventLoader?
// used only for the initial load
// persists the raw data in the DB
class LoadEvents {
  constructor (app) {
    this.app = app;
  }

  // add a comment summarizing what it does
  async start () {
    try {
      this.app.logger.info("getting past event to find SuperTokens");
      const systemInfo = await this.app.db.models.SystemModel.findOne();
      //TODO: we should pick the last block number from the last event (CFA or GDA)
      const lastEventBlockNumber = await this.app.db.models.FlowUpdatedModel.findOne({
        order: [["blockNumber", "DESC"]]
      });
      let blockNumber = lastEventBlockNumber === null
        ? parseInt(this.app.config.EPOCH_BLOCK)
        : lastEventBlockNumber.blockNumber;

      if (systemInfo !== null) {
        blockNumber = Math.max(blockNumber, systemInfo.superTokenBlockNumber);
        const currentChainId = await this.app.client.getChainId();
        if (currentChainId !== systemInfo.chainId) {
          throw new Error("different network than from the saved data");
        }
      }

      let pullCounter = blockNumber;
      const currentBlockNumber = await this.app.client.getCurrentBlockNumber(this.app.config.BLOCK_OFFSET);
      const realBlockNumber = currentBlockNumber + this.app.config.BLOCK_OFFSET;

      this.app.logger.info(`scanning blocks from ${pullCounter} to ${currentBlockNumber} - real ${realBlockNumber}`);
      const queue = async.queue(async function (task) {
        let keepTrying = 1;
        while (true) {
          try {
            task.self.app.logger.info(`getting blocks: trying #${keepTrying} - from:${task.fromBlock} to:${task.toBlock}`);

            const query = task.self.app.config.TOKENS
                ? { filter: { token: task.self.app.config.TOKENS }, fromBlock: task.fromBlock, toBlock: task.toBlock }
                : { fromBlock: task.fromBlock, toBlock: task.toBlock };

            let CFAFlowUpdated = await task.self.app.protocol.getCFAAgreementEvents("FlowUpdated", query);
            let GDAFlowDistributionUpdated = await task.self.app.protocol.getGDAgreementEvents("FlowDistributionUpdated", query);
            CFAFlowUpdated = CFAFlowUpdated.map(task.self.app.models.event.transformWeb3Event);
            CFAFlowUpdated.forEach(result => result.source = "CFA");
            GDAFlowDistributionUpdated = GDAFlowDistributionUpdated.map(task.self.app.models.event.transformWeb3Event);
            GDAFlowDistributionUpdated.forEach(result => result.source = "GDA");

            // debug only ----------------------------------
            let GDAPoolCreated = await task.self.app.protocol.getGDAgreementEvents("PoolCreated", query);
            GDAPoolCreated = GDAPoolCreated.map(task.self.app.models.event.transformWeb3Event);
            GDAPoolCreated.forEach(result => result.source = "GDA-PoolCreated");
            for(const event of GDAPoolCreated) {
              const agreementId = await task.self.app.protocol.generateGDAId(event.admin, event.pool);
              await task.self.app.db.models.PoolCreatedModel.create({
                agreementId: agreementId,
                address: event.address,
                blockNumber: event.blockNumber,
                superToken: event.token,
                admin: event.admin,
                pool: event.pool,
              });
            }
            // end debug only ----------------------------------

            const events = [...CFAFlowUpdated, ...GDAFlowDistributionUpdated];
            for (const event of events) {
              if (event.source === "CFA") {
                const agreementId = task.self.app.protocol.generateCFAId(event.sender, event.receiver);
                const hashId = task.self.app.protocol.generateCFAId(event.token, agreementId);

                await task.self.app.db.models.FlowUpdatedModel.create({
                  address: event.address,
                  blockNumber: event.blockNumber,
                  superToken: event.token,
                  sender: event.sender,
                  receiver: event.receiver,
                  flowRate: event.flowRate,
                  agreementId: agreementId,
                  hashId: hashId
                });
              } else {
                const agreementId = await task.self.app.protocol.generateGDAId(event.distributor, event.pool);
                await task.self.app.db.models.FlowDistributionModel.create({
                  agreementId: agreementId,
                  superToken: event.token,
                  pool: event.pool,
                  distributor: event.distributor,
                  operator: event.operator,
                  oldFlowRate: event.oldFlowRate,
                  newDistributorToPoolFlowRate: event.newDistributorToPoolFlowRate,
                  newPoolToDistributorFlowRate: event.newPoolToDistributorFlowRate,
                  newTotalDistributionFlowRate: event.newTotalDistributionFlowRate,
                  adjustmentFlowRate: event.adjustmentFlowRate,
                  blockNumber: event.blockNumber
                });
              }
            }

            break;
          } catch (err) {
            keepTrying++;
            task.self.app.logger.error(err);
            // this often happens due to RPC rate limiting, thus it's wise to add some delay here
            await task.self.app.timer.timeout(keepTrying * 1000); // linear backoff
            if (keepTrying > task.self.app.config.NUM_RETRIES) {
              process.exit(1);
            }
          }
        }
      }, this.app.config.CONCURRENCY);
      //
      while (pullCounter <= currentBlockNumber) {
        const end = (pullCounter + parseInt(this.app.config.MAX_QUERY_BLOCK_RANGE));
        queue.push({self: this, fromBlock: pullCounter, toBlock: end > currentBlockNumber ? currentBlockNumber : end });
        pullCounter = end + 1;
      }

      await queue.drain();

      const tokensCFA = await this.app.db.models.FlowUpdatedModel.findAll({
        attributes: ["superToken"],
        group: ["superToken"]
      });
      const tokensGDA = await this.app.db.models.FlowDistributionModel.findAll({
        attributes: ["superToken"],
        group: ["superToken"]
      });
      const tokens = [...tokensCFA, ...tokensGDA];
      // now remove those in config.EXCLUDED_TOKENS, and log which were excluded
      const excludedTokens = this.app.config.EXCLUDED_TOKENS;
      const filteredTokens = tokens.filter(token => !excludedTokens.includes(token.superToken));
      this.app.logger.info(`found ${tokens.length} tokens, excluding ${excludedTokens.length} tokens, keeping ${filteredTokens.length} tokens`);
      // fresh database
      if (systemInfo === null) {
        await this.app.db.models.SystemModel.create({
          blockNumber: blockNumber,
          chainId: await this.app.client.getChainId(),
          superTokenBlockNumber: currentBlockNumber
        });
      } else {
        systemInfo.superTokenBlockNumber = currentBlockNumber;
        await systemInfo.save();
      }
      // Load supertokens
      await this.app.client.superToken.loadSuperTokens(tokens.map(({ superToken }) => superToken));
      if(!this.app.config.OBSERVER) {
        this.app.logger.info("start getting delays PIC system");
        // we need to query each supertoken to check pic address
        const delayChecker = async.queue(async function (task) {
          let keepTrying = 10;
          while (true) {
            try {
              await task.self.app.protocol.calculateAndSaveTokenDelay(task.token, false);
              break;
            } catch (err) {
              keepTrying++;
              task.self.app.logger.error(err);
              if (keepTrying > task.self.app.config.NUM_RETRIES) {
                task.self.app.logger.error(`exhausted number of retries`);
                process.exit(1);
              }
            }
          }
        }, this.app.config.CONCURRENCY);
        const superTokens = this.app.client.superToken.superTokensAddresses;
        for (const st of superTokens) {
          delayChecker.push({
            self: this,
            token: st
          });
        }

        if (superTokens.length > 0 && !this.app.config.OBSERVER) {
          await delayChecker.drain();
        }
        this.app.logger.info("finish getting delays PIC system");
      } else {
        this.app.logger.info("running as observer, ignoring PIC system");
      }

      this.app.logger.info("finish past event to find SuperTokens");
      return currentBlockNumber;
    } catch (err) {
      this.app.logger.error(err);
      process.exit(1);
    }
  }
}

module.exports = LoadEvents;
