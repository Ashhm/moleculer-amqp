# moleculer-amqp

Task queue mixin for [AMQP](http://www.squaremobius.net/amqp.node/)

## Description
In order to divide RabbitMQ working queues and transport queues, this mixin was created. Also, it can be used with any [moleculer transport](https://moleculer.services/docs/0.13/networking.html#Transporters) you want.


## Installation

```bash
$ npm install moleculer-amqp --save
```

## Usage
#### Simple queue handler with validation
```js
const amqpMixin = require('moleculer-amqp');

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
    // Assign to greeting queue
    greetings: {
      // Params validation with moleculer validator
      params: {
        text: { type: 'string' },
      },
      // Error strategy defines how to nack message on error
      errorStrategy: {
        requeue: false,
      },
      // Handler will receive decoded message on consume
      handler({ text }) {
        console.log('Hello from amqp with new message:', text);
      },
    },
  },
});
```

# Test
```
$ npm test
```

In development with watching

```
$ npm run ci
```


## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.

## License
[MIT](https://choosealicense.com/licenses/mit/)
