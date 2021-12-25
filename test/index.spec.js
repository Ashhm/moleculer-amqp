'use strict';

const amqplib = require('amqplib-mocks');
const chai = require('chai');
const chaiAsPromised = require('chai-as-promised');
const proxyquire = require('proxyquire');
const sinon = require('sinon');
const sinonChai = require('sinon-chai');
const { Errors, ServiceBroker, Service } = require('moleculer');

const amqpMixin = proxyquire('../src', { amqplib });
const url = 'amqp://localhost';
const { expect } = chai;
chai.should();
chai.use(chaiAsPromised);
chai.use(sinonChai);

describe('AMQP mixin', () => {
  const broker = new ServiceBroker({ logger: false });
  const mixinMethods = [
    'acceptMessage',
    'processMessage',
    'rejectMessage',
    'rpc',
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
    const simpleQueueHandler = sinon.stub();
    const replyToQueueHandler = sinon.stub();
    const withValidatorQueueHandler = sinon.stub();
    const errorStrategy = sinon.stub();
    const schema = {
      name: 'test',
      settings: {
        rpcTimeout: 1000,
      },
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
        replyTo: {
          handler: replyToQueueHandler,
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
    const simpleMessage = { simple: true };
    const replyToMessage = { replyTo: true };
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
        .and.satisfy(mixins => mixinMethods
          .every(method => expect(mixins[method])
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

      after('clear spy history', () => {
        simpleQueueHandler.resetHistory();
      });

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

    describe('rpc', () => {
      describe('publish to exchange', () => {
        before('bind queue to exchange',
          () => service.channel.bindQueue('simple', 'direct', 'routingKey'));

        before('mock simpleQueueHandler', () => {
          simpleQueueHandler.callsFake((payload, { correlationId, replyTo, messageTTL }) => {
            service.sendToQueue(replyTo, replyToMessage, { correlationId, messageTTL });
          });
        });

        before('rpc', () => {
          service.rpc(
            simpleMessage,
            { exchangeName: 'direct', routingKey: 'routingKey', replyTo: 'replyTo', messageTTL: '1000' },
          );
        });

        after('clear spy history', () => {
          simpleQueueHandler.reset();
          replyToQueueHandler.resetHistory();
        });

        it('should call simpleQueueHandler on message appear in simple queue', async () => {
          simpleQueueHandler.should.have.been.calledWith(simpleMessage);
          return simpleQueueHandler.should.have.been.calledOnce;
        });

        it('should call replyToQueueHandler on message appear in replyTo queue', () => {
          replyToQueueHandler.should.have.been.calledWith(replyToMessage);
          return replyToQueueHandler.should.have.been.calledOnce;
        });
      });

      describe('publish to queue', () => {
        before('mock simpleQueueHandler', () => {
          simpleQueueHandler.callsFake((payload, { correlationId, replyTo, messageTTL }) => {
            service.sendToQueue(replyTo, replyToMessage, { correlationId, messageTTL });
          });
        });

        before('rpc', () => {
          service.rpc(
            simpleMessage,
            { queueName: 'simple', replyTo: 'replyTo', messageTTL: '1000' },
          );
        });

        after('clear spy history', () => {
          simpleQueueHandler.reset();
          replyToQueueHandler.resetHistory();
        });

        it('should call simpleQueueHandler on message appear in simple queue', async () => {
          simpleQueueHandler.should.have.been.calledWith(simpleMessage);
          return simpleQueueHandler.should.have.been.calledOnce;
        });

        it('should call replyToQueueHandler on message appear in replyTo queue', () => {
          replyToQueueHandler.should.have.been.calledWith(replyToMessage);
          return replyToQueueHandler.should.have.been.calledOnce;
        });
      });

      describe('request timeout error', () => {
        let promise;

        before('rpc', () => {
          promise = service.rpc(
            simpleMessage,
            { queueName: 'simple', replyTo: 'replyTo' },
          );
        });

        after('clear spy history', () => {
          simpleQueueHandler.resetHistory();
          replyToQueueHandler.resetHistory();
        });

        it('should be rejected with request timeout error', () => (
          promise.should.be.rejectedWith(Errors.MoleculerRetryableError, 'Request timeout')
        ));

        it('should call simpleQueueHandler on message appear in simple queue', async () => {
          simpleQueueHandler.should.have.been.calledWith(simpleMessage);
          return simpleQueueHandler.should.have.been.calledOnce;
        });

        it('should not call replyToQueueHandler', () => (
          replyToQueueHandler.should.not.have.been.called
        ));
      });
    });
  });

  describe('Reconnection mechanism', () => {
    const schema = {
      name: 'reconnection',
      mixins: [amqpMixin(url)],
    };
    let connection;
    let clock;
    let service;

    before('use fake timers', () => {
      clock = sinon.useFakeTimers();
    });

    after('restore timers', () => {
      clock.restore();
    });

    describe('on service hooks', () => {
      before('create a service', async () => {
        service = await broker.createService(schema);
      });

      before('add stubs and spies', () => {
        sinon.spy(service, '_connect');
        sinon.spy(service, '_reconnect');
        sinon.spy(service, '_setup');
      });

      describe('created', () => {
        after('clear spies', () => {
          sinon.resetHistory();
        });

        it('should not call amqplib connect',
          () => expect(amqplib.connect).to.not.have.been.calledOnce);
      });

      describe('started', () => {
        before('run broker', () => broker.start());

        before('get connection', () => {
          ({ channel: { connection } } = service);
        });

        after('clear spies', () => {
          amqplib.connect.resetHistory();
          sinon.resetHistory();
        });

        it('should call service _setup method',
          () => expect(service._setup).to.have.been.calledOnce);

        it('should not call service _reconnect method',
          () => expect(service._reconnect).to.not.have.been.called);
      });

      describe('stopped', () => {
        before('stop broker', () => broker.stop());

        after('clear spies', () => {
          amqplib.connect.resetHistory();
          sinon.resetHistory();
        });

        after('destroy service', () => broker.destroyService(service));

        it('should not call connect after reconnection timeout', () => {
          clock.tick(service.metadata.reconnectTimeout);
          return expect(service._connect).to.not.have.been.called;
        });

        it('should not call amqplib connect',
          () => expect(amqplib.connect).to.not.have.been.called);

        it('should set service channel to null', () => expect(service.channel).to.be.null);
      });
    });

    describe('on connection closed', () => {
      before('create a service', async () => {
        service = await broker.createService(schema);
        return broker.start();
      });

      before('add stubs and spies', () => {
        sinon.spy(service, '_connect');
        sinon.spy(service, '_reconnect');
        sinon.spy(service, '_setup');
      });

      before('close amqp connection', () => {
        service.channel.connection.close();
      });

      after('clear spies', () => {
        amqplib.connect.resetHistory();
        sinon.resetHistory();
      });

      after('destroy service', () => Promise
        .all([broker.stop(), broker.destroyService(service)]));

      it('should set service channel to null',
        () => expect(service.channel).to.be.null);

      it('should call _reconnect', () => expect(service._reconnect).to.have.been.called);

      it('should not call _connect', () => expect(service._connect).to.not.have.been.called);

      it('should call amqplib connect only once',
        () => expect(amqplib.connect).to.have.been.calledOnce);
    });

    describe('on connection error', () => {
      before('create a service', async () => {
        service = await broker.createService(schema);
        return broker.start();
      });

      before('add stubs and spies', () => {
        sinon.spy(service, '_connect');
        sinon.spy(service, '_reconnect');
        sinon.spy(service, '_setup');
      });

      before('get connection and call extended connection error method', () => {
        ({ channel: { connection } } = service);
        connection.closeWithError = sinon.stub().callsFake(() => {
          connection.on.withArgs('error').yield();
          connection.close();
        });
        connection.closeWithError();
      });

      after('clear spies', () => {
        amqplib.connect.resetHistory();
        sinon.resetHistory();
      });

      after('destroy service', () => Promise
        .all([broker.stop(), broker.destroyService(service)]));

      it('should set service channel to null', () => expect(service.channel).to.be.null);

      it('should call _reconnect',
        () => expect(service._reconnect).to.have.been.calledOnce);

      it('should call _connect after reconnection timeout', () => {
        clock.tick(service.metadata.reconnectTimeout);
        return expect(service._connect).to.have.been.calledOnce;
      });

      it('should not call _setup', () => expect(service._setup).to.not.have.been.calledOnce);

      // TODO: Implement valid tests
      /* it('should not call connect after next reconnection timeout', () => {
        clock.tick(service.metadata.reconnectTimeout);
        return expect(service._connect).to.not.have.been.calledTwice
          && expect(service._connect).to.have.been.calledOnce;
      }); */

      it('should call amqplib connect',
        () => expect(amqplib.connect).to.have.been.called);

      // it('should set service channel', () => expect(service.channel).to.exist);
    });

    // TODO: Implement valid tests
    /* describe('on multiple errors', () => {
      before('create a service', async () => {
        service = await broker.createService(schema);
        return broker.start();
      });

      before('add stubs and spies', () => {
        sinon.spy(service, '_connect');
        sinon.spy(service, '_reconnect');
        sinon.spy(service, '_setup');
        amqplib.connect.onCall(1).callsFake(() => Promise.reject());
        amqplib.connect.onCall(2).callsFake(() => Promise.reject());
      });

      before('get connection and call extended connection error method', () => {
        ({ channel: { connection } } = service);
        connection.closeWithError = sinon.stub().callsFake(() => {
          connection.on.withArgs('error').yield();
          connection.close();
        });
        connection.closeWithError();
      });

      after('clear spies', () => {
        amqplib.connect.resetHistory();
        sinon.resetHistory();
      });

      after('destroy service', () => Promise
        .all([broker.stop(), broker.destroyService(service)]));

      it('should set service channel to null', () => expect(service.channel).to.be.null);

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

      it('should set service channel', () => expect(service.channel).to.exist);
    }); */
  });
});
