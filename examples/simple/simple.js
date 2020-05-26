'use strict';

const { ServiceBroker } = require('moleculer/index');

const amqpMixin = require('../../index');

const broker = new ServiceBroker({ logger: console, namespace: 'development', transporter: 'AMQP' });

broker.createService({
  name: 'greeting',
  settings: {
    rpcTimeout: 5000,
  },
  mixins: [amqpMixin('amqp://localhost')],
  actions: {
    rpcDemo: {
      async handler() {
        const exchangeName = 'exchange01';
        const routingKey = 'routingKey';
        const queueName = 'queue01';
        const replyTo = 'queueReplyTo';
        const fakeRequestData = { fake: true };
        const fakeResponseData = { fake: true };

        await this.channel.bindQueue(queueName, exchangeName, routingKey);

        this.channel.consume(queueName, async (message) => {
          await this.sendToQueue(replyTo, fakeResponseData, message.properties);
        });

        const opts = { exchangeName, routingKey, replyTo };
        return this.rpc(fakeRequestData, opts);
      },
    },
    sendGreeting: {
      params: {
        text: { type: 'string', min: 2 },
      },
      handler(ctx) {
        const { params: text } = ctx;
        return this.sendToQueue('greetings', text, { persistent: true });
      },
    },
  },
  exchanges: {
    exchange01: {
      type: 'direct',
      durable: true,
    },
  },
  queues: {
    queue01: {
      queueOpts: {
        exclusive: true,
        messageTtl: 10 * 1000,
      },
    },
    queueReplyTo: {
      queueOpts: {
        exclusive: true,
        messageTtl: 10 * 1000,
      },
    },
    greetings: {
      params: {
        text: { type: 'string' },
      },
      errorStrategy: {
        requeue: false,
      },
      handler({ text }) {
        // eslint-disable-next-line no-console
        console.log('Hello from amqp with new message:', text);
      },
    },
  },
});

broker.start();
