const { createLogger, format, transports } = require("winston");
const { combine, timestamp, label, printf } = format;

class Logger {

    constructor(app) {
        this.app = app;
        this.logs = new Array();
        const logFormat = printf(({ level, message, label, timestamp }) => {
            return `${timestamp} - ${level}: ${message}`;
        });
        this.logger = createLogger({
            format: combine(label({ label: this.level }), timestamp(), logFormat),
            transports: [
                new transports.Console({
                    level: "info",
                    handleExceptions: true,
                    json: false,
                    colorize: true,
                })
            ]
        });
    }

    log(message) {
        try {
            this.logger.log("info", message);
        } catch(err) {
            console.error(err);
        }
    }

    info(message) {
        try {
            this.logger.info(message);
        } catch(err) {
            console.error(err);
        }
    }

    debug(message) {
        try {
            this.logger.debug(message);
        } catch(err) {
            console.error(err);
        }
    }

    error(message) {
        try {
            this.logger.error(message);
            if(this.app.config.shutdownOnError) {
                this.app.shutdown();
            }
        } catch(err) {
            console.error(err);
        }
    }

    warn(message) {
        try {
            this.logger.warn(message);
        } catch(err) {
            console.error(err);
        }
    }
}

module.exports = Logger;