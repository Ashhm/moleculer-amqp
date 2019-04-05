'use strict';

const amqplib = require('amqplib');
const { ValidationError } = require('moleculer');

/**
 * AMQP mixin
 *
 * Provide work tasks queues for rabbitMQ
 *
 * @param {String} url RabbitMQ connection string
 * @param {Object} options Connection socket options
 * @returns {*}
 */
module.exports = function createService(url, options) {
  return {
    name: 'amqp',
    methods: {
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
       * /**
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
            await handler(payload);
            await this.acceptMessage(message);
          } catch (error) {
            this.logger.error(error);
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
     * Service created hook
     * @returns {Promise<void>}
     */
    async created() {
      const connection = await amqplib.connect(url, options);
      this.channel = await connection.createChannel();
    },

    /**
     * Service started hook
     * @returns {Promise<*>}
     */
    async started() {
      if (this.schema.queues) {
        await Promise.all(Object.entries(this.schema.queues)
          .map(async ([queueName, options]) => {
            const { assertOnly, prefetch, queueOpts = { durable: true } } = options;
            await this.channel.assertQueue(queueName, queueOpts);
            if (!assertOnly) {
              if (prefetch) {
                await this.channel.prefetch(prefetch);
              }
              this.channel.consume(queueName, this.processMessage(options));
            }
          }));
      }
      if (this.schema.exchanges) {
        await Promise.all(Object.entries(this.schema.exchanges)
          .map(async ([exchangeName, options]) => {
            const { exchangeOpts = { durable: true }, type = 'direct' } = options;
            await this.channel.assertExchange(exchangeName, type, exchangeOpts);
          }));
      }
      return this.Promise.resolve();
    },

    /**
     * Service stopped hook
     * @returns {Promise<*>}
     */
    async stopped() {
      await this.channel.connection.close();
    },
  };
};
