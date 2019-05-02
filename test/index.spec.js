'use strict';

const amqplib = require('amqplib-mocks');
const chai = require('chai');
const proxyquire = require('proxyquire');
const sinon = require('sinon');
const sinonChai = require('sinon-chai');
const { ServiceBroker, Service } = require('moleculer');

const amqpMixin = proxyquire('../src', { amqplib });
const url = 'amqp://localhost';
const { expect } = chai;
chai.should();
chai.use(sinonChai);

describe('AMQP mixin', () => {
  const broker = new ServiceBroker({ logger: false });
  const mixinMethods = [
    'acceptMessage',
    'processMessage',
    'rejectMessage',
    'sendToQueue',
    'validate',
  ];

  describe('As service', () => {
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

    after('destroy service', () => Promise
      .all([broker.stop(), broker.destroyService(service)]));

    after('clear spies', () => {
      amqplib.connect.resetHistory();
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

  describe('Reconnection mechanism', () => {
    const schema = {
      name: 'reconnection',
      mixins: [amqpMixin(url)],
    };
    let connection;
    let channel;
    let clock;
    let service;
    let spies;

    before('create a service', async () => {
      service = await broker.createService(schema);
    });

    before('get connection and channel', () => {
      setImmediate(() => {
        ({ channel, channel: { connection } } = service);
      });
    });

    before('add stubs and spies', () => {
      spies = [
        sinon.spy(service, '_connect'),
        sinon.spy(service, '_setup'),
      ];
    });

    before('use fake timers', () => {
      clock = sinon.useFakeTimers();
    });

    describe('on service created', () => {
      after('clear spies', () => {
        amqplib.connect.resetHistory();
      });

      it('should call amqplib connect',
        () => expect(amqplib.connect).to.have.been.calledOnce);

      it('connection and channel should be defined',
        () => expect(connection).to.exist && expect(channel).to.exist);

      it('should not call channel prefetch',
        () => expect(channel.prefetch).to.not.have.been.calledOnce);
    });

    describe('on service started', () => {
      before('run broker', () => broker.start());

      after('clear spies', () => {
        service._setup.resetHistory();
      });

      it('should call service _setup method',
        () => expect(service._setup).to.have.been.calledOnce);
    });

    describe('on service stopped', () => {
      before('stop broker', () => broker.stop());

      after('clear spies', () => {
        amqplib.connect.resetHistory();
        service._connect.resetHistory();
      });

      after('destroy service', () => broker.destroyService(service));

      it('should call connect after reconnection timeout', () => {
        clock.tick(service.metadata.reconnectTimeout);
        return expect(service._connect).to.have.been.calledOnce;
      });

      it('should not call connect after next reconnection timeout', () => {
        clock.tick(service.metadata.reconnectTimeout);
        return expect(service._connect).to.have.been.calledOnce;
      });

      it('should not call amqplib connect',
        () => expect(amqplib.connect).to.not.have.been.called);

      it('should not set service channel to null', () => expect(service.channel).not.to.be.null);
    });

    describe('on connection closed', () => {
      before('create a service', async () => {
        service = await broker.createService(schema);
        return broker.start();
      });

      before('add stubs and spies', () => {
        spies.push(sinon.spy(service, '_connect'));
      });

      before('close amqp connection', () => {
        setImmediate(() => {
          service.channel.connection.close();
        });
      });

      after('clear spies', () => {
        amqplib.connect.resetHistory();
        service._connect.resetHistory();
      });

      after('destroy service', () => Promise
        .all([broker.stop(), broker.destroyService(service)]));

      it('should not set service channel to null', () => {
        clock.tick(100);
        return expect(service.channel).not.to.be.null;
      });

      it('should call connect after reconnection timeout', () => {
        clock.tick(service.metadata.reconnectTimeout);
        return expect(service._connect).to.have.been.calledOnce;
      });

      it('should not call connect after next reconnection timeout', () => {
        clock.tick(service.metadata.reconnectTimeout);
        return expect(service._connect).to.have.been.calledOnce;
      });

      it('should call amqplib connect',
        () => expect(amqplib.connect).to.have.been.called);
    });

    describe('on connection error', () => {
      before('create a service', async () => {
        service = await broker.createService(schema);
        return broker.start();
      });

      before('add stubs and spies', () => {
        spies.push(sinon.spy(service, '_connect'));
      });

      before('get connection and call extended connection error method', () => {
        setImmediate(() => {
          ({ channel: { connection } } = service);
          connection.closeWithError = sinon.stub().callsFake(() => {
            connection.on.withArgs('error').yield();
            connection.close();
          });
          connection.closeWithError();
        });
      });

      after('clear spies', () => {
        amqplib.connect.resetHistory();
        service._connect.resetHistory();
      });

      after('destroy service', () => Promise
        .all([broker.stop(), broker.destroyService(service)]));

      it('should set service channel to null', () => {
        clock.tick(100);
        return expect(service.channel).to.be.null;
      });

      it('should call connect after reconnection timeout', () => {
        clock.tick(service.metadata.reconnectTimeout);
        return expect(service._connect).to.have.been.calledOnce;
      });

      it('should not call connect after next reconnection timeout', () => {
        clock.tick(service.metadata.reconnectTimeout);
        return expect(service._connect).to.have.been.calledOnce;
      });

      it('should call amqplib connect',
        () => expect(amqplib.connect).to.have.been.called);

      it('should set service channel', () => expect(service.channel).to.exist);
    });

    describe('on multiply errors', () => {
      before('create a service', async () => {
        service = await broker.createService(schema);
        return broker.start();
      });

      before('add stubs', () => {
        spies.push(sinon.spy(service, '_connect'));
      });

      before('emulate errors on amqp connect', () => {
        amqplib.connect.onCall(1).rejects();
        amqplib.connect.onCall(2).rejects();
      });

      before('get connection and call extended connection error method', () => {
        setImmediate(() => {
          ({ channel: { connection } } = service);
          connection.closeWithError = sinon.stub().callsFake(() => {
            connection.on.withArgs('error').yield();
            connection.close();
          });
          connection.closeWithError();
        });
      });

      after('clear spies', () => {
        amqplib.connect.resetHistory();
        service._connect.resetHistory();
      });

      after('destroy service', () => Promise
        .all([broker.stop(), broker.destroyService(service)]));

      it('should set service channel to null', () => {
        clock.tick(100);
        return expect(service.channel).to.be.null;
      });

      it('should call connect after first reconnection timeout', () => {
        clock.tick(service.metadata.reconnectTimeout);
        return expect(service._connect).to.have.been.calledOnce;
      });

      it('should call connect after second reconnection timeout', () => {
        clock.tick(service.metadata.reconnectTimeout);
        return expect(service._connect).to.have.been.calledTwice;
      });

      it('should call connect after third reconnection timeout', () => {
        clock.tick(service.metadata.reconnectTimeout);
        return expect(service._connect).to.have.been.calledThrice;
      });

      it('should not call connect after fourth reconnection timeout', () => {
        clock.tick(service.metadata.reconnectTimeout);
        return expect(service._connect).to.have.been.calledThrice;
      });

      it('should call amqplib connect',
        () => expect(amqplib.connect).to.have.been.called);

      it('should set service channel', () => expect(service.channel).to.exist);
    });
  });
});
