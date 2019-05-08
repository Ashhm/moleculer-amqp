'use strict';

const { ServiceBroker } = require('moleculer/index');

const amqpMixin = require('../../index');

const broker = new ServiceBroker({ logger: console, namespace: 'development', transporter: 'AMQP' });

broker.createService({
  name: 'greeting',
  mixins: [amqpMixin('amqp://localhost')],
  actions: {
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
  queues: {
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
