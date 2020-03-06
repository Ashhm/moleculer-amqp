'use strict';

const amqplib = require('amqplib');
const {
  Errors: {
    ValidationError,
  },
  ServiceSchemaError,
} = require('moleculer');
/**
 * AMQP mixin
 *
 * Provide work tasks queues for rabbitMQ
 *
 * @param {String} url RabbitMQ connection string
 * @param {Object} socketOptions Connection socket options
 * @returns {*}
 */
module.exports = function createService(url, socketOptions) {
  return {
    name: 'amqp',
    metadata: {
      reconnectTimeout: 5000,
    },
    methods: {
      /**
       * Connect to rabbitMQ with socketOptions.
       * Reconnect is supported
       *
       * @returns {Promise<void>}
       * @private
       */
      async _connect() {
        try {
          this.logger.info('Connecting to the transporter with AMQP mixin...');
          const connection = await amqplib.connect(url, socketOptions);
          this.logger.info('AMQP mixin is connected.');
          this._isReconnecting = false;

          connection.on('error', (err) => {
            this.channel = null;
            this.logger.warn('AMQP mixin connection error.', err);
          });

          connection.on('close', () => {
            this.channel = null;
            this.logger.info('AMQP mixin connection is closed.');
            if (!this._isStoped) {
              this._reconnect();
            }
          });

          this.channel = await connection.createChannel();
          this.logger.info('AMQP mixin channel is created');
          await this._setup();
        } catch (err) {
          this.channel = null;
          this._isReconnecting = false;
          this.logger.warn('Connection with AMQP mixin is failed.', err);
          this._reconnect();
        }
      },

      /**
       * Reconnect to rabbitMQ with timeout
       *
       * @private
       */
      _reconnect() {
        if (!this._isReconnecting) {
          this._isReconnecting = true;
          this.logger.info('Reconnecting with AMQP mixin...');
          setTimeout(this._connect, this.metadata.reconnectTimeout);
        }
      },

      /**
       * Setup channel queues and exchanges
       *
       * @returns {Promise<*>}
       * @private
       */
      async _setup() {
        try {
          this.queues = {};
          const { queues, exchanges, channel = {} } = this.schema;
          await this.channel.prefetch(channel.prefetch || 1);

          if (exchanges) {
            await this.Promise.all(Object.entries(this.schema.exchanges)
              .map(async ([exchangeName, options]) => {
                const { exchangeOpts = { durable: true }, type = 'direct' } = options;
                await this.channel.assertExchange(exchangeName, type, exchangeOpts);
              }));
          }

          if (queues) {
            await this.Promise.all(Object.entries(queues)
              .map(async ([queueName, options]) => {
                const {
                  queueOpts = { durable: true },
                  randomName,
                  bindings,
                  ...restOptions
                } = options;
                const name = randomName ? '' : queueName;
                const { queue } = await this.channel.assertQueue(name, queueOpts);
                this.queues[queueName] = queue;
                if (restOptions.handler) {
                  if (!(restOptions.handler instanceof Function)) {
                    const message = 'Queue handler should be a function';
                    throw new ServiceSchemaError(message, { queueName, options });
                  }
                  this.channel.consume(queueName, this.processMessage(restOptions));
                }
                if (bindings) {
                  await this.Promise.all(
                    Object.entries(bindings)
                      .map(([exchangeName, key]) => this
                        .channel.bindQueue(name, exchangeName, key)));
                }
              }));
          }

          // TODO: Support channel binds
          return this.Promise.resolve();
        } catch (err) {
          this.logger.error(err);
          return this.Promise.reject(err);
        }
      },

      /**
       * Send message to exchange
       *
       * @param {String} exchange
       * @param {String} routingKey
       * @param {Object} message
       * @param {Object} options
       * @returns {*|PromiseLike<void>|{headers, ticket, messageId, clusterId,
       * priority, type, mandatory, userId, immediate, deliveryMode, appId,
       * replyTo, contentEncoding, exchange, correlationId, expiration,
       * contentType, routingKey, timestamp}}
       *
       * @link https://www.squaremobius.net/amqp.node/channel_api.html#channel_publish
       */
      publish(exchange = '', routingKey, message, options) {
        return this.channel
          .publish(exchange, routingKey, Buffer.from(JSON.stringify(message)), options);
      },

      /**
       * Send message to queue
       *
       * @param {String} queueName
       * @param {Object} message
       * @param {Object} options
       * @returns {*|PromiseLike<void>|{headers, ticket, messageId, clusterId,
       * priority, type, mandatory, userId, immediate, deliveryMode, appId,
       * replyTo, contentEncoding, exchange, correlationId, expiration,
       * contentType, routingKey, timestamp}}
       */
      sendToQueue(queueName, message, options) {
        return this.channel
          .sendToQueue(queueName, Buffer.from(JSON.stringify(message)), options);
      },

      /**
       * Accept message
       *
       * @param {Object} message
       * @param {Boolean[]} args
       * @returns {void|*|{deliveryTag, multiple}}
       *
       * @link http://www.squaremobius.net/amqp.node/channel_api.html#channel_ack
       */
      acceptMessage(message, ...args) {
        return this.channel.ack(message, ...args);
      },

      /**
       * Reject message
       *
       * @param {Object} message
       * @param {Boolean} args
       * @returns {{requeue, deliveryTag, multiple}|void|*}
       *
       * @link http://www.squaremobius.net/amqp.node/channel_api.html#channel_nack
       */
      rejectMessage(message, ...args) {
        return this.channel.nack(message, ...args);
      },

      /**
       * Validate message
       *
       * @param {Object} payload
       * @param {Object} params params it's validation schema
       * it can be defined for each queue as params field
       * @returns {*}
       */
      validate(payload, params) {
        const { options: { validation }, validator } = this.broker;
        if (validation && params) {
          const check = validator.compile(params);
          const result = check(payload);
          if (result === true) {
            return this.Promise.resolve();
          } else {
            return this.Promise
              .reject(new ValidationError('Parameters validation error!', null, result));
          }
        }
        return this.Promise.resolve();
      },

      /**
       * Process consumed message
       *
       * Return a function to handle received message:
       * 1. Parse message content
       * 2. Validate parsed message content via built-in validator
       * 3. Pass parsed message content to handler
       * 4. Accept message or handle an error and reject message with given strategy
       *
       * @param {Object} options
       * @returns {Function}
       */
      processMessage(options) {
        const { handler, errorStrategy, params } = options;
        return async (message) => {
          let payload;
          try {
            const { serializer } = this.broker;
            payload = serializer.deserialize(message.content.toString());
            await this.validate(payload, params);
            await handler.call(this, payload);
            await this.acceptMessage(message);
          } catch (error) {
            this.logger.debug(error);
            // Error handler may change a rejection strategy
            const result = typeof errorStrategy === 'function'
              ? await errorStrategy.call(this, error, payload) : errorStrategy;
            const { allUpTo = false, requeue = true } = result || {};
            await this.rejectMessage(message, allUpTo, requeue);
          }
        };
      },
    },


    /**
     * Service started hook
     * @returns {Promise<void>}
     */
    async started() {
      this._isStoped = false;
      await this._connect();
    },

    /**
     * Service stopped hook
     * @returns {Promise<void>}
     */
    async stopped() {
      this._isStoped = true;
      await this.channel.connection.close();
    },
  };
};
