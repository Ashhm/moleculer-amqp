'use strict';

module.exports = {
  extends: 'airbnb-base',
  rules: {
    'class-methods-use-this': 0,
    'consistent-return': 'warn',
    'function-paren-newline': 0,
    'import/no-self-import': 0,
    'no-else-return': 0,
    'no-param-reassign': 0,
    'no-shadow': 'warn',
    'no-underscore-dangle': 0,
    'object-curly-newline': 0,
  },

  overrides: [
    {
      files: ['test/*.js'],
      rules: {
        'func-names': 0,
        'import/no-extraneous-dependencies': ['error', { devDependencies: true }],
        'mocha/no-exclusive-tests': 'error',
        'mocha/no-identical-title': 'error',
        'mocha/no-pending-tests': 'warn',
      },
      env: {
        mocha: true,
      },
      plugins: [
        'mocha',
      ],
    }
  ],

  parserOptions: {
    sourceType: 'script',
  },

  settings: {
    'import/resolver': {
      node: {
        moduleDirectory: [
          'node_modules',
          '.',
        ]
      }
    }
  }
};
