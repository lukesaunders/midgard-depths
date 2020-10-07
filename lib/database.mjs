import Knex from 'knex';
const environment = process.env.NODE_ENV || 'development'
const config = require('../knexfile.js')[environment];
const knex = Knex(config);
export default knex;
