'use strict';

const uuid = require('uuid');
const amqplib = require('amqplib');
const {
  Errors: {
    MoleculerError,
    MoleculerRetryableError,
    ServiceSchemaError,
    ValidationError,
  },
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
    settings: {
      rpcTimeout: 5000,
    },
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
          const { queues, exchanges, channelOpts = {} } = this.schema;
          await this.channel.prefetch(channelOpts.prefetch || 1);

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
       * @returns {Boolean}
       * @throws {ValidationError}
       */
      validate(payload, params) {
        const { validator } = this.broker;
        const isValidationEnabled = !!this.broker.options.validator;
        if (isValidationEnabled && params) {
          const check = validator.compile(params);
          const result = check(payload);
          if (result === true) {
            return true;
          } else {
            throw new ValidationError('Parameters validation error!', null, result);
          }
        }
        return true;
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
            this.validate(payload, params);
            await handler.call(this, payload, message.properties);
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

      /**
       * Disconnect consumer
       *
       * @private
       *
       * @param {Object} consumer Consumer
       * @param {String} consumer.consumerTag Consumer tag
       */
      async disconnectConsumer(consumer) {
        if (consumer && consumer.consumerTag) {
          try {
            await this.channel.cancel(consumer.consumerTag);
          } catch (err) {
            this.logger.warn('An error occurred during disconnecting consumer', err);
          }
        }
      },

      /**
       * RPC
       *
       * @param {Object} payload Payload to send to the queue
       * @param {Object} options Options for transport
       * @param {String} options.exchangeName Exchange name
       * @param {String} options.routingKey Routing key
       * @param {String} options.queueName Queue name, used w/o exchange
       * @param {String} options.replyTo Queue to put reply to
       * @returns {Promise<*>}
       */
      rpc(payload, { exchangeName, routingKey = '', queueName, replyTo }) {
        return new Promise(async (resolve, reject) => {
          const correlationId = uuid.v4();
          let timeout;

          try {
            const consumer = await this.channel.consume(replyTo, async (message) => {
              // Once consumer is canceled by RabbitMQ
              // callback is invoked with a message equals null
              if (message) {
                if (message.properties.correlationId === correlationId) {
                  try {
                    resolve(this.broker.serializer.deserialize(message.content.toString()));
                  } catch (err) {
                    reject(err);
                  } finally {
                    clearTimeout(timeout);
                    this.acceptMessage(message);
                    // Consumer callback may be fired even before we get its data
                    const { consumerTag } = message.fields;
                    await this.disconnectConsumer({ consumerTag });
                  }
                } else {
                  this.rejectMessage(message);
                }
              }
            });

            const options = { correlationId, persistent: true, replyTo };
            const isSent = exchangeName
              ? this.publish(exchangeName, routingKey, payload, options)
              : this.sendToQueue(queueName, payload, options);

            if (!isSent) {
              await this.disconnectConsumer(consumer);
              return reject(new MoleculerError(
                'Message has not been published',
                424,
                '',
                payload,
              ));
            }

            if (this.settings.rpcTimeout) {
              timeout = setTimeout(
                async () => {
                  await this.disconnectConsumer(consumer);
                  reject(new MoleculerRetryableError('Request timeout', 504));
                },
                this.settings.rpcTimeout,
              );
            }
          } catch (err) {
            reject(err);
          }

          return true;
        });
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
      if (this.channel) {
        await this.channel.connection.close();
      }
    },
  };
};
