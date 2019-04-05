'use strict';

const amqplib = require('amqplib-mocks');
const chai = require('chai');
const proxyquire = require('proxyquire');
const sinon = require('sinon');
const { ServiceBroker, Service } = require('moleculer');
const sinonChai = require('sinon-chai');

const amqpMixin = proxyquire('../src', { amqplib });
const url = 'amqp://localhost';
const { expect } = chai;
chai.should();
chai.use(sinonChai);

describe('AMQP queues', () => {
  const broker = new ServiceBroker({ logger: false });
  const mixinMethods = [
    'acceptMessage',
    'processMessage',
    'rejectMessage',
    'sendToQueue',
    'validate',
  ];

  describe('As service', async () => {
    let service;

    before('create a service', async () => {
      service = await broker.createService(amqpMixin(url));
    });

    it('should be created', () => expect(service).to.exist);

    it('should be an instance of Service',
      () => expect(service).to.be.an.instanceOf(Service));

    after('destroy service', () => broker.destroyService(service));
  });

  describe('As mixin', () => {
    const simpleQueueHandler = sinon.spy();
    const withValidatorQueueHandler = sinon.spy();
    const errorStrategy = sinon.spy();
    const schema = {
      name: 'test',
      mixins: [amqpMixin(url)],
      queues: {
        simple: {
          handler: simpleQueueHandler,
          prefetch: 1,
        },
        withOpts: {
          handler: () => {},
          queueOpts: {
            durable: true,
            autoDelete: true,
          },
        },
        withValidator: {
          errorStrategy,
          handler: withValidatorQueueHandler,
          params: {
            a: 'string',
            b: 'string',
          },
          prefetch: 1,
        },
      },
      exchanges: {
        direct: {
          type: 'direct',
          exchangeOpts: {
            durable: true,
          },
        },
        fanout: {
          type: 'fanout',
          exchangeOpts: {
            autoDelete: true,
          },
        },
        topic: {
          type: 'topic',
          exchangeOpts: {
            durable: true,
            internal: true,
          },
        },
        headers: {
          type: 'headers',
        },
      },
    };
    const simpleMessage = {};
    let service;

    before('create a service', async () => {
      service = await broker.createService(schema);
      return broker.start();
    });

    it('should be created', () => expect(service).to.exist);

    it('should be an instance of Service',
      () => expect(service).to.be.an.instanceof(Service));

    it('all mixin methods should be available', () => {
      expect(service)
        .to.be.an('object')
        .that.include.all.keys(mixinMethods)
        .and.satisfy(service => mixinMethods
          .every(method => expect(service[method])
            .to.be.a('function')));
    });

    describe('assert to exchanges', () => {
      it('should call assertExchange channel method 4 times', () => {
        service.channel.assertExchange.should.have.callCount(4);
      });
    });

    describe('publish to queue', () => {
      before('bind queue to exchange',
        () => service.channel.bindQueue('simple', 'direct', 'routingKey'));

      before('publish message', () => {
        service.publish('direct', 'routingKey', simpleMessage);
      });

      after('clear spy history', () => {
        simpleQueueHandler.resetHistory();
      });

      it('should call simpleQueueHandler on message appear in simple queue', () => {
        simpleQueueHandler.should.have.been.calledWith(simpleMessage);
        return simpleQueueHandler.should.have.been.calledOnce;
      });
    });

    describe('consume messages from queue', () => {
      before('send message to simple queue',
        () => service.sendToQueue('simple', simpleMessage));

      before('wait for event processing', done => setTimeout(done, 500));

      it('should call simpleQueueHandler on message appear in simple queue', () => {
        simpleQueueHandler.should.have.been.calledWith(simpleMessage);
        return simpleQueueHandler.should.have.been.calledOnce;
      });
    });

    describe('create custom queues with options', () => {
      it('should set proper options for withOpts', () => {
        // Not sure that this should work with real amqplib
        const { connection: { queues: { withOpts } } } = service.channel;
        expect(withOpts)
          .to.be.an('object')
          .that.has.property('options')
          .which.is.an('object')
          .and.deep.includes({ durable: true, autoDelete: true });
      });
    });

    describe('consume message from queue with validation', () => {
      const wrongMessage = { a: 1, b: 2 };

      before('send message to withValidator queue',
        () => service.sendToQueue('withValidator', wrongMessage));

      before('wait for event processing', done => setTimeout(done, 500));

      it('should call errorStrategy on message appear in simple queue', () => {
        const { args } = errorStrategy.getCall(0);
        expect(args[0]).to.be.instanceof(Error);
        expect(args[1]).to.deep.equal(wrongMessage);
        return errorStrategy.should.have.been.calledOnce;
      });
    });
  });
});
