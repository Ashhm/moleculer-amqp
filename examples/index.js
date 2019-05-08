'use strict';

const moduleName = process.argv[2] || 'simple';
process.argv.splice(2, 1);

// eslint-disable-next-line import/no-dynamic-require
module.exports = require(`./${moduleName}`);
