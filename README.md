# Superfluid Community Solvency Agent

- This agent uses both HTTP and WS connection to EVM node.
- Use sqlite as database layer.
- As this version the parameters are passing as env. variables.


### Installation and boot


1. Clone the repo
   ```sh
   git clone https://github.com/ngmachado/community-solvency.git
   ```
2. Install NPM packages
   ```sh
   npm install
   ```

3. Install pm2 (optional)
   ```sh
    npm install -g pm2
   ```

4. Set configuration file `.env`
   ```sh
   cat env_template > .env
   ```
5. Edit `.env` file
    ```sh
        HTTP_NODE = HTTP NODE URL
        WS_NODE = WS NODE URL
        EPOCH_BLOCK= START BLOCK OF SYSTEM
        MNEMONIC= YOUR MNEMONIC
        PROTOCOL_RELEASE_VERSION=v1
        TIMEOUT_FN = 300000
        PULL_STEP = 3000000
        GAS_PRICE = 5000000000
        CONCURRENCY=1
        DB= PATH TO DB
        COLD_BOOT=1
        LISTEN_MODE=1
    ```
5. Run  `main.js`
    ```sh
    node main
    ```

## More boot options

Set two agents to Polygon and xDAI
    ```sh
    npm run pm2:prod
    ```

_Check `package.json` to see more options_



## Concepts

_Cold Boot_: Fresh database at boot. (Old information will be deleted)

_Critical Account_: Flows that can be terminated.

_Insolvent Account_: Sender account is negative.

_Listed SuperTokens_: SuperTokens register in Superfluid resolver contract.

_Non Listed SuperTokens_: SuperTokens not register in Superfluid resolver contract.


# Code structure

[TODO]

# Flow of Agent
Depending on the database information, the agent will try to collect all the needed information from the network.

1.  Load FlowUpdated Events

2.  Get all SuperTokens Addresses

3.  Get all Accounts (senders and receiver)

4.  Estimate liquidation point for each account.

5.  Periodically get all estimations that are ready to be send, check account solvency status, build and send transaction.


# Design options

## General

This agent should be self sufficient without depending on external services. The only external services is the EVM node.

The node need to expose HTTP(S) and WS(S) connection, the agent will use both.

## Failure

Each node call to collect information will retry the same operation by default seven times before exit the process with error.

## Reboot

After the initial boot phase the agent subscribe to SuperTokens events and operate based on that information. As one double check, if the process exit the reboot will take in consideration the last sucessful boot blockNumber and restart from that point (double checking all the events).

## Liquidation transaction

For each agreement is necessary to perform a liquidation, in this version we are managing only one agent account.
When the agent submit one transaction it start a timeout clock, when the timeout trigger we resubmit the same transaction (same nonce) with a higher gasPrice.

## Gas Price Estimation

We are now testing the web3 gasEstimation function to give us a base layer that we can work.
---
