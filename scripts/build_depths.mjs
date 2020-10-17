import axios from 'axios';
import numeral from 'numeral';
import knex from '../lib/database.mjs';

process.on('unhandledRejection', up => { throw up });
const debug = false;

async function fetchNodeDepth({ height, timestamp }) {
  const response = await axios.get('http://chaosnet.rune:1317/thorchain/pool/bnb.bnb', { params: { height }});
  return response.data['balance_rune'];
}

async function fetchMidgardDepthLogPromise({ height, timestamp }) {
  // select rune_e8 from block_pool_depths where block_timestamp = 1602017868601953886 and pool = 'BNB.BNB';
  const blockPoolDepth = await knex('block_pool_depths').
    select('rune_e8').
    where({ pool: 'BNB.BNB' }).
    whereRaw('block_timestamp <= ?', timestamp).
    first();
  if (debug) console.log(blockPoolDepth);
  if (!blockPoolDepth) return 0;
  return blockPoolDepth['rune_e8'];
}

async function buildTotalForBlock({ height, timestamp }) {
  // select sum(rune_e8) from stake_events where pool = 'BNB.BNB';
  const { totalstakes } = await knex('stake_events').
    select(knex.raw('sum(rune_e8) as totalStakes')).
    where({ pool: 'BNB.BNB'}).
    whereRaw('stake_events.block_timestamp <= ?', timestamp).
    first();
  if (debug) console.log({ totalstakes });

  // - select sum(oe.asset_e8) from outbound_events oe join unstake_events ue on (ue.tx = oe.in_tx) where oe.asset = 'BNB.RUNE-B1A' and ue.pool = 'BNB.BNB';
  const { totalunstakes } = await knex('outbound_events').
    joinRaw('join unstake_events ue on (ue.tx = outbound_events.in_tx)').
    where({ 'outbound_events.asset': 'BNB.RUNE-B1A', 'ue.pool': 'BNB.BNB' }).
    whereRaw('ue.block_timestamp <= ?', timestamp).
    select(knex.raw('sum(outbound_events.asset_e8) as totalunstakes')).
    first();
  if (debug) console.log({ totalunstakes });

  // select sum(from_e8) from swap_events where pool = 'BNB.BNB' and from_asset = 'BNB.RUNE-B1A';
  const { swapins } = await knex('swap_events').
    select(knex.raw('sum(from_e8) as swapins')).
    where({ pool: 'BNB.BNB', from_asset: 'BNB.RUNE-B1A'}).
    whereRaw('swap_events.block_timestamp <= ?', timestamp).
    first();
  if (debug) console.log({ swapins });

  // - select sum(asset_e8) from outbound_events oe join swap_events se on (se.tx = oe.in_tx) where oe.asset = 'BNB.RUNE-B1A' and se.pool = 'BNB.BNB' and se.from_asset != 'BNB.RUNE-B1A';
  const { swapouts } = await knex('outbound_events').
    joinRaw('join swap_events se on (se.tx = outbound_events.in_tx)').
    where({ 'outbound_events.asset': 'BNB.RUNE-B1A', 'se.pool': 'BNB.BNB' }).
    whereNot({ 'from_asset': 'BNB.RUNE-B1A' }).
    whereRaw('se.block_timestamp <= ?', timestamp).
    select(knex.raw('sum(outbound_events.asset_e8) as swapouts')).
    first();
  if (debug) console.log({ swapouts });

  // - select sum(asset_e8) from fee_events where asset = 'BNB.RUNE-B1A' and tx in (select tx from fee_events fe where fe.asset = 'BNB.BNB');
  const { runefee } = await knex('fee_events').
    where({ asset: 'BNB.RUNE-B1A'}).
    whereRaw('tx in (select tx from fee_events fe where fe.asset = ?)', 'BNB.BNB').
    whereRaw('fee_events.block_timestamp <= ?', timestamp).
    select(knex.raw('sum(fee_events.asset_e8) as runefee')).
    first();
  if (debug) console.log({ runefee });

  // - select sum(fe.pool_deduct) from fee_events fe where fe.asset = 'BNB.BNB';
  const { pooldeduct } = await knex('fee_events').
    where({ asset: 'BNB.BNB' }).
    select(knex.raw('sum(fee_events.pool_deduct) as pooldeduct')).
    whereRaw('fee_events.block_timestamp <= ?', timestamp).
    first();
  if (debug) console.log({ pooldeduct });

  // select sum(fe.asset_e8) from fee_events fe join refund_events re on (re.tx = fe.tx) where fe.asset = 'BNB.RUNE-B1A' and re.asset = 'BNB.BNB';
  const { refundfees } = await knex('fee_events').
    where({ 'fee_events.asset': 'BNB.RUNE-B1A' }).
    joinRaw('join refund_events re on (re.tx = fee_events.tx)').
    where({ 're.asset': 'BNB.BNB' }).
    select(knex.raw('sum(fee_events.asset_e8) as refundfees')).
    whereRaw('fee_events.block_timestamp <= ?', timestamp).    
    first();
  if (debug) console.log({ refundfees });

  // select sum(fe.asset_e8) from fee_events fe join swap_events se on (se.tx = fe.tx) where fe.asset = 'BNB.RUNE-B1A' and se.pool = 'BNB.BNB';
  const { swapfees } = await knex('fee_events').
    where({ 'fee_events.asset': 'BNB.RUNE-B1A' }).
    joinRaw('join swap_events se on (se.tx = fee_events.tx)').
    where({ 'se.pool': 'BNB.BNB' }).
    select(knex.raw('sum(fee_events.asset_e8) as swapfees')).
    whereRaw('fee_events.block_timestamp <= ?', timestamp).    
    first();
  if (debug) console.log({ swapfees });

  // select sum(fe.asset_e8) from fee_events fe join swap_events se on (se.tx = fe.tx) where fe.asset = 'BNB.RUNE-B1A' and se.pool = 'BNB.BNB';
  const { unstakefees } = await knex('fee_events').
    where({ 'fee_events.asset': 'BNB.RUNE-B1A' }).
    joinRaw('join unstake_events ue on (ue.tx = fee_events.tx)').
    where({ 'ue.pool': 'BNB.BNB' }).
    select(knex.raw('sum(fee_events.asset_e8) as unstakefees')).
    whereRaw('fee_events.block_timestamp <= ?', timestamp).    
    first();
  if (debug) console.log({ unstakefees });

  // select sum(rune_e8) from add_events where pool = 'BNB.BNB';
  const { adds } = await knex('add_events').
    where({ pool: 'BNB.BNB' }).
    select(knex.raw('sum(rune_e8) as adds')).
    whereRaw('add_events.block_timestamp <= ?', timestamp).    
    first();
  if (debug) console.log({ adds });

  // select sum(rune_e8) from rewards_event_entries where pool = 'BNB.BNB';
  const { rewards } = await knex('rewards_event_entries').
    where({ pool: 'BNB.BNB' }).
    select(knex.raw('sum(rune_e8) as rewards')).
    whereRaw('rewards_event_entries.block_timestamp <= ?', timestamp).
    first();
  if (debug) console.log({ rewards });

  // select sum(rune_e8) from gas_events where asset = 'BNB.BNB';
  const { gas } = await knex('gas_events').
    where({ asset: 'BNB.BNB' }).
    select(knex.raw('sum(rune_e8) as gas')).
    whereRaw('gas_events.block_timestamp <= ?', timestamp).
    first();

  if (debug) console.log({ gas });

  const totalDepth = parseInt(totalstakes || 0) - parseInt(totalunstakes || 0) + parseInt(swapins || 0) - parseInt(swapouts || 0) - parseInt(pooldeduct || 0) - parseInt(swapfees || 0) - parseInt(unstakefees || 0) + parseInt(adds || 0) + parseInt(rewards || 0) + parseInt(gas || 0);
  if (debug) console.log('TOTAL DEPTH: ', totalDepth);

  return totalDepth;

  /*
    const [
    { totalstakes },
    { totalunstakes },
    { swapins },
    { swapouts },
    { runefee },
    { pooldeduct },
    { refundfees },
    { adds },
    { rewards },
    { gas }
  ] = Promise.all([
    stakePromise,
    unstakePromise,
    swapInPromise,
    swapOutPromise,
    runeFeePromise,
    poolDeductPromise,
    refundFeePromise,
    addsPromise,
    rewardsPromise,
    gasPromise,
  ]);
  */
}

async function outputForHeight({ height }) {
  const { timestamp } = await knex('block_log').select('timestamp').where({ height }).first();
  const calculatedDepthPromise = buildTotalForBlock({ height, timestamp });
  const nodeDepthPromise = fetchNodeDepth({ height, timestamp });
  const midgardDepthLogPromise = fetchMidgardDepthLogPromise({ height, timestamp });
  const [calculatedDepth, nodeDepth, midgardDepthLog] = await Promise.all([calculatedDepthPromise, nodeDepthPromise, midgardDepthLogPromise]);
  return([
    height,
    timestamp,
    numeral(calculatedDepth).format('0,0'),
    numeral(nodeDepth).format('0,0'),
    numeral((calculatedDepth - nodeDepth)).format('0,0'),
    numeral(midgardDepthLog).format('0,0'),
    numeral((calculatedDepth - midgardDepthLog)).format('0,0'),
  ]);
}

async function run() {
  console.log('run');
  let promises = []
  const startHeight = process.env['START_HEIGHT'] || 700000;
  for (let height = startHeight; height < 716800; height++) {
    const outputPromise = outputForHeight({ height });
    promises.push(outputPromise);
    if (promises.length == 1) {
      const results = await Promise.all(promises);
      for (const returnVals of results) {
        console.log(returnVals.join(' '));        
      }
      promises = [];
    }
  }
  knex.destroy();
}

run();
