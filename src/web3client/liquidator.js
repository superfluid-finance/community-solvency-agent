class Liquidator {
  constructor (app) {
    this.app = app;
    this._isShutdown = false;
  }

  async start () {
    try {
      if (this.app._isShutdown) {
        this._isShutdown = true;
        this.app.logger.info(`app.shutdown() - closing liquidation`);
        return;
      }
      this.app.logger.debug(`running liquidation job`);
      const checkDate = this.app.time.getTimeWithDelay(0);
      let haveBatchWork = [];
      // if we have a batchLiquidator contract, use batch calls
      if (this.app.config.BATCH_CONTRACT !== undefined) {
        haveBatchWork = await this.app.db.queries.getNumberOfBatchCalls(checkDate);
        this.app.logger.debug(JSON.stringify(haveBatchWork));
      }
      if (haveBatchWork.length > 0) {
        await this.multiTermination(haveBatchWork, checkDate);
      } else {
        const work = await this.app.db.queries.getLiquidations(checkDate, this.app.config.TOKENS, this.app.config.MAX_TX_NUMBER);
        await this.singleTerminations(work);
      }
    } catch (err) {
      this.app.logger.error(`liquidator.start() - ${err}`);
      return {
        error: err,
        msg: undefined
      };
    }

    return {
      error: undefined,
      msg: "ended"
    };
  }

  async isPossibleToClose (superToken, sender, receiver) {
    // Note: If flow does not exist on the network, we are going to remove from DB
    return (await this.app.protocol.checkFlow(superToken, sender, receiver)) !== undefined &&
      (await this.app.protocol.isAccountCriticalNow(superToken, sender));
  }

  async singleTerminations (work) {
    const wallet = this.app.client.getAccount();
    const chainId = await this.app.client.getChainId();
    const networkAccountNonce = await this.app.client.web3.eth.getTransactionCount(wallet.address);
    for (const job of work) {
      if (await this.isPossibleToClose(job.superToken, job.sender, job.receiver)) {
        try {
          const tx = this.app.protocol.generateDeleteFlowABI(job.superToken, job.sender, job.receiver);
          const BaseGasPrice = await this.app.gasEstimator.getGasPrice();
          const txObject = {
            retry: 1,
            step: this.app.config.RETRY_GAS_MULTIPLIER,
            target: this.app.client.sf._address,
            flowSender: job.sender,
            flowReceiver: job.receiver,
            superToken: job.superToken,
            tx: tx,
            gasPrice: BaseGasPrice.gasPrice,
            nonce: networkAccountNonce,
            chainId: chainId
          };
          const result = await this.sendWithRetry(wallet, txObject, this.app.config.TX_TIMEOUT);
          if (result !== undefined && result.error !== undefined) {
            this.app.logger.error(result.error);
          } else {
            this.app.logger.debug(JSON.stringify(result));
          }
        } catch (err) {
          this.app.logger.error(err);
          process.exit(1);
        }
      } else {
        this.app.logger.debug(`address ${job.sender} is solvent at ${job.superToken}`);
        await this.app.queues.addQueuedEstimation(job.superToken, job.sender, "Liquidation job");
        await this.app.timer.timeout(500);
      }
    }
  }

  async multiTermination (batchWork, checkDate) {
    for (const batch of batchWork) {
      let senders = [];
      let receivers = [];
      const streams = await this.app.db.queries.getLiquidations(
        checkDate,
        batch.superToken,
        this.app.config.MAX_TX_NUMBER
      );

      for (const flow of streams) {
        if (await this.isPossibleToClose(flow.superToken, flow.sender, flow.receiver)) {
          senders.push(flow.sender);
          receivers.push(flow.receiver);
        } else {
          this.app.logger.debug(`address ${flow.sender} is solvent at ${flow.superToken}`);
          await this.app.queues.addQueuedEstimation(flow.superToken, flow.sender, "Liquidation job");
          await this.app.timer.timeout(500);
        }

        if (senders.length === this.app.config.MAX_BATCH_TX) {
          if (senders.length === parseInt(this.app.config.MAX_BATCH_TX)) {
            this.app.logger.debug(`sending a full batch work: load ${senders.length}`);
            await this.sendBatch(batch.superToken, senders, receivers);
            senders = [];
            receivers = [];
          }
        }
      }

      if (senders.length !== 0) {
        if (senders.length === 1) {
          await this.singleTerminations([{
            superToken: batch.superToken,
            sender: senders[0],
            receiver: receivers[0]
          }]);
        } else {
          this.app.logger.debug(`sending a partial batch work: load ${senders.length}`);
          await this.sendBatch(batch.superToken, senders, receivers);
        }
      }
    }
  }

  async sendBatch (superToken, senders, receivers) {
    const wallet = this.app.client.getAccount();
    const chainId = await this.app.client.getChainId();
    const networkAccountNonce = await this.app.client.web3.eth.getTransactionCount(wallet.address);
    try {
      const tx = this.app.protocol.generateMultiDeleteFlowABI(superToken, senders, receivers);
      const BaseGasPrice = await this.app.gasEstimator.getGasPrice();
      const txObject = {
        retry: 1,
        step: this.app.config.RETRY_GAS_MULTIPLIER,
        target: this.app.config.BATCH_CONTRACT,
        superToken: superToken,
        tx: tx,
        gasPrice: BaseGasPrice.gasPrice,
        nonce: networkAccountNonce,
        chainId: chainId
      };
      const result = await this.sendWithRetry(wallet, txObject, this.app.config.TX_TIMEOUT);
      if (result !== undefined && result.error !== undefined) {
        this.app.logger.error(result.error);
      } else {
        this.app.logger.debug(JSON.stringify(result));
      }
    } catch (err) {
      this.app.logger.error(err);
      process.exit(1);
    }
  }

  async sendWithRetry (wallet, txObject, ms) {
    await this.app.timer.timeout(1000);
    // When estimate gas we get a preview of what can happen when send the transaction. Depending on the error we should execute specific logic
    const gas = await this.app.gasEstimator.getGasLimit(wallet, txObject);
    if (gas.error !== undefined) {
      if (gas.error instanceof this.app.Errors.ExecutionReverted) {
        await this.app.protocol.checkFlow(txObject.superToken, txObject.flowSender, txObject.flowReceiver);
      }

      return {
        error: gas.error,
        tx: undefined
      };
    }

    txObject.gasLimit = gas.gasLimit;
    const signed = await this.signTx(wallet, txObject);
    if (signed.error !== undefined) {
      const error = this.app.Errors.EVMErrorParser(signed.error);
      if(error instanceof this.app.Errors.TxUnderpricedError) {
        this.app.logger.warn(`replacement transaction underpriced`);
        txObject.retry++;
        return this.sendWithRetry(wallet, txObject, ms);
      }
    if(error instanceof this.app.Errors.ExecutionReverted) {
        this.app.logger.warn(error.originalMessage);
        return {
          error: error,
          tx: undefined
        };
      }

      return {
        error: signed.error,
        tx: undefined
      };
    }

    try {
      txObject.txHash = signed.tx.transactionHash;
      signed.tx.timeout = ms;
      this.app.logger.info(`waiting until timeout for ${ms / 1000} seconds for tx ${txObject.txHash}`);
      // Broadcast transaction
      const tx = await this.app.timer.promiseTimeout(
        this.app.client.sendSignedTransaction(signed),
        ms
      );

      return {
        error: undefined,
        tx: tx
      };
    } catch (err) {
      if(err instanceof this.app.Errors.TimeoutError) {
        this.app.logger.debug(`timeout of tx: ${signed.tx.transactionHash}`);
        txObject.retry++;
        return this.sendWithRetry(wallet, txObject, ms);
      }
      //get errors from EVM
      const error = this.app.Errors.EVMErrorParser(err);
      if(error instanceof this.app.Errors.TxUnderpricedError) {
        this.app.logger.warn(`replacing transaction underpriced`);
        txObject.retry++;
        return this.sendWithRetry(wallet, txObject, ms);
      }
      if(error instanceof this.app.Errors.AccountNonceError) {
        this.app.logger.warn(`nonce too low, retry`);
        txObject.nonce++;
        return this.sendWithRetry(wallet, txObject, ms);
      }
      if(error instanceof this.app.Errors.TxAlreadyKnownError) {
        this.app.logger.warn(`submitted tx already known`);
        return {
          error: error.message,
          tx: undefined
        };
      }
      if(error instanceof this.app.Errors.TxUnderpricedError) {
        this.app.logger.warn(`insufficient funds agent account`);
        return {
          error: error.message,
          tx: undefined
        };
      }
      if(error instanceof this.app.Errors.GasBlockLimitError) {
        this.app.logger.warn(`exceeds block gas limit`);
        this.app.config.MAX_BATCH_TX = Math.ceil(parseInt(this.app.config.MAX_BATCH_TX / 2));
        this.app.logger.warn(`reducing batch size to ${this.app.config.MAX_BATCH_TX}`);
        if (this.app.config.MAX_BATCH_TX < 1) {
          this.app.logger.warn(`can't reduce batch size more...`);
          process.exit(1);
        }
        return {
          error: error.message,
          tx: undefined
        };
      }
      // log remaining errors
      this.app.logger.error(`Liquidator.sendWithRetry() - no logic to catch error : ${error}`);
    }
  }

  async signTx (wallet, txObject) {
    try {
      txObject.gasPrice = this.app.gasEstimator.getUpdatedGasPrice(txObject.gasPrice, txObject.retry, txObject.step);
      const unsignedTx = {
        chainId: txObject.chainId,
        to: txObject.target,
        from: wallet.address,
        data: txObject.tx,
        nonce: txObject.nonce,
        gasPrice: txObject.gasPrice,
        gasLimit: txObject.gasLimit
      };
      const signed = await this.app.client.signTransaction(
        unsignedTx,
        wallet._privateKey.toString("hex")
      );
      signed.txObject = txObject;
      return {
        tx: signed,
        error: undefined
      };
    } catch (err) {
      return {
        tx: undefined,
        error: err
      };
    }
  }
}

module.exports = Liquidator;
