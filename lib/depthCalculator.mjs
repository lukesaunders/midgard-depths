import knex from './database.mjs';

export async function runeDepthForHeightAndPool({ timestamp, pool, debug = false }) {
  // select sum(rune_e8) from stake_events where pool = pool;
  const { totalstakes } = await knex('stake_events').
    select(knex.raw('sum(rune_e8) as totalStakes')).
    where({ pool: pool}).
    whereRaw('stake_events.block_timestamp <= ?', timestamp).
    first();
  if (debug) console.log({ totalstakes });

  // - select sum(oe.asset_e8) from outbound_events oe join unstake_events ue on (ue.tx = oe.in_tx) where oe.asset = 'BNB.RUNE-B1A' and ue.pool = pool;
  const { totalunstakes } = await knex('outbound_events').
    joinRaw('join unstake_events ue on (ue.tx = outbound_events.in_tx)').
    where({ 'outbound_events.asset': 'BNB.RUNE-B1A', 'ue.pool': pool }).
    whereRaw('ue.block_timestamp <= ?', timestamp).
    select(knex.raw('sum(outbound_events.asset_e8) as totalunstakes')).
    first();
  if (debug) console.log({ totalunstakes });

  // select sum(from_e8) from swap_events where pool = pool and from_asset = 'BNB.RUNE-B1A';
  const { swapins } = await knex('swap_events').
    select(knex.raw('sum(from_e8) as swapins')).
    where({ pool: pool, from_asset: 'BNB.RUNE-B1A'}).
    whereRaw('swap_events.block_timestamp <= ?', timestamp).
    first();
  if (debug) console.log({ swapins });

  // - select sum(asset_e8) from outbound_events oe join swap_events se on (se.tx = oe.in_tx) where oe.asset = 'BNB.RUNE-B1A' and se.pool = pool and se.from_asset != 'BNB.RUNE-B1A';
  const { swapouts } = await knex('outbound_events').
    joinRaw('join swap_events se on (se.tx = outbound_events.in_tx)').
    where({ 'outbound_events.asset': 'BNB.RUNE-B1A', 'se.pool': pool }).
    whereNot({ 'from_asset': 'BNB.RUNE-B1A' }).
    whereRaw('se.block_timestamp <= ?', timestamp).
    select(knex.raw('sum(outbound_events.asset_e8) as swapouts')).
    first();
  if (debug) console.log({ swapouts });

  // - select sum(asset_e8) from fee_events where asset = 'BNB.RUNE-B1A' and tx in (select tx from fee_events fe where fe.asset = pool);
  const { runefee } = await knex('fee_events').
    where({ asset: 'BNB.RUNE-B1A'}).
    whereRaw('tx in (select tx from fee_events fe where fe.asset = ?)', pool).
    whereRaw('fee_events.block_timestamp <= ?', timestamp).
    select(knex.raw('sum(fee_events.asset_e8) as runefee')).
    first();
  if (debug) console.log({ runefee });

  // - select sum(fe.pool_deduct) from fee_events fe where fe.asset = pool;
  const { pooldeduct } = await knex('fee_events').
    where({ asset: pool }).
    select(knex.raw('sum(fee_events.pool_deduct) as pooldeduct')).
    whereRaw('fee_events.block_timestamp <= ?', timestamp).
    first();
  if (debug) console.log({ pooldeduct });

  // select sum(fe.asset_e8) from fee_events fe join refund_events re on (re.tx = fe.tx) where fe.asset = 'BNB.RUNE-B1A' and re.asset = pool;
  const { refundfees } = await knex('fee_events').
    where({ 'fee_events.asset': 'BNB.RUNE-B1A' }).
    joinRaw('join refund_events re on (re.tx = fee_events.tx)').
    where({ 're.asset': pool }).
    select(knex.raw('sum(fee_events.asset_e8) as refundfees')).
    whereRaw('fee_events.block_timestamp <= ?', timestamp).    
    first();
  if (debug) console.log({ refundfees });

  // select sum(fe.asset_e8) from fee_events fe join swap_events se on (se.tx = fe.tx) where fe.asset = 'BNB.RUNE-B1A' and se.pool = pool;
  const { swapfees } = await knex('fee_events').
    where({ 'fee_events.asset': 'BNB.RUNE-B1A' }).
    joinRaw('join swap_events se on (se.tx = fee_events.tx)').
    where({ 'se.pool': pool }).
    select(knex.raw('sum(fee_events.asset_e8) as swapfees')).
    whereRaw('fee_events.block_timestamp <= ?', timestamp).    
    first();
  if (debug) console.log({ swapfees });

  // select sum(fe.asset_e8) from fee_events fe join swap_events se on (se.tx = fe.tx) where fe.asset = 'BNB.RUNE-B1A' and se.pool = pool;
  const { unstakefees } = await knex('fee_events').
    where({ 'fee_events.asset': 'BNB.RUNE-B1A' }).
    joinRaw('join unstake_events ue on (ue.tx = fee_events.tx)').
    where({ 'ue.pool': pool }).
    select(knex.raw('sum(fee_events.asset_e8) as unstakefees')).
    whereRaw('fee_events.block_timestamp <= ?', timestamp).    
    first();
  if (debug) console.log({ unstakefees });

  // select sum(rune_e8) from add_events where pool = pool;
  const { adds } = await knex('add_events').
    where({ pool: pool }).
    select(knex.raw('sum(rune_e8) as adds')).
    whereRaw('add_events.block_timestamp <= ?', timestamp).    
    first();
  if (debug) console.log({ adds });

  // select sum(rune_e8) from rewards_event_entries where pool = pool;
  const { rewards } = await knex('rewards_event_entries').
    where({ pool: pool }).
    select(knex.raw('sum(rune_e8) as rewards')).
    whereRaw('rewards_event_entries.block_timestamp <= ?', timestamp).
    first();
  if (debug) console.log({ rewards });

  // select sum(rune_e8) from gas_events where asset = pool;
  const { gas } = await knex('gas_events').
    where({ asset: pool }).
    select(knex.raw('sum(rune_e8) as gas')).
    whereRaw('gas_events.block_timestamp <= ?', timestamp).
    first();

  if (debug) console.log({ gas });

  const totalDepth = parseInt(totalstakes || 0) - parseInt(totalunstakes || 0) + parseInt(swapins || 0) - parseInt(swapouts || 0) - parseInt(pooldeduct || 0) - parseInt(swapfees || 0) - parseInt(unstakefees || 0) + parseInt(adds || 0) + parseInt(rewards || 0) + parseInt(gas || 0);
  if (debug) console.log('TOTAL DEPTH: ', totalDepth);

  return totalDepth;
}


export async function assetDepthForHeightAndPool({ timestamp, pool, debug = false }) {
  const { totalstakes } = await knex('stake_events').
    select(knex.raw('sum(asset_e8) as totalStakes')).
    where({ pool: pool}).
    whereRaw('stake_events.block_timestamp <= ?', timestamp).
    first();
  if (debug) console.log({ totalstakes });

  const { totalunstakes } = await knex('outbound_events').
    joinRaw('join unstake_events ue on (ue.tx = outbound_events.in_tx)').
    where({ 'outbound_events.asset': pool, 'ue.pool': pool }).
    whereRaw('ue.block_timestamp <= ?', timestamp).
    select(knex.raw('sum(outbound_events.asset_e8) as totalunstakes')).
    first();
  if (debug) console.log({ totalunstakes });

  // select sum(from_e8) from swap_events where pool = pool and from_asset = 'BNB.RUNE-B1A';
  const { swapins } = await knex('swap_events').
    select(knex.raw('sum(from_e8) as swapins')).
    where({ pool: pool, from_asset: pool}).
    whereRaw('swap_events.block_timestamp <= ?', timestamp).
    first();
  if (debug) console.log({ swapins });

  const { swapouts } = await knex('outbound_events').
    joinRaw('join swap_events se on (se.tx = outbound_events.in_tx)').
    where({ 'outbound_events.asset': pool, 'se.pool': pool }).
    whereNot({ 'from_asset': pool }).
    whereRaw('se.block_timestamp <= ?', timestamp).
    select(knex.raw('sum(outbound_events.asset_e8) as swapouts')).
    first();
  if (debug) console.log({ swapouts });

  const { feeassetamt } = await knex('fee_events').
    where({ asset: pool }).
    select(knex.raw('sum(fee_events.asset_e8) as feeassetamt')).
    whereRaw('fee_events.block_timestamp <= ?', timestamp).
    first();
  if (debug) console.log({ feeassetamt });

  const { refundfees } = await knex('fee_events').
    where({ 'fee_events.asset': pool }).
    joinRaw('join refund_events re on (re.tx = fee_events.tx)').
    where({ 're.asset': pool }).
    select(knex.raw('sum(fee_events.asset_e8) as refundfees')).
    whereRaw('fee_events.block_timestamp <= ?', timestamp).    
    first();
  if (debug) console.log({ refundfees });

  const { swapfees } = await knex('fee_events').
    where({ 'fee_events.asset': pool }).
    joinRaw('join swap_events se on (se.tx = fee_events.tx)').
    where({ 'se.pool': pool }).
    select(knex.raw('sum(fee_events.asset_e8) as swapfees')).
    whereRaw('fee_events.block_timestamp <= ?', timestamp).    
    first();
  if (debug) console.log({ swapfees });

  const { unstakefees } = await knex('fee_events').
    where({ 'fee_events.asset': pool }).
    joinRaw('join unstake_events ue on (ue.tx = fee_events.tx)').
    where({ 'ue.pool': pool }).
    select(knex.raw('sum(fee_events.asset_e8) as unstakefees')).
    whereRaw('fee_events.block_timestamp <= ?', timestamp).    
    first();
  if (debug) console.log({ unstakefees });

  const { adds } = await knex('add_events').
    where({ pool: pool }).
    select(knex.raw('sum(asset_e8) as adds')).
    whereRaw('add_events.block_timestamp <= ?', timestamp).    
    first();
  if (debug) console.log({ adds });

  const { gas } = await knex('gas_events').
    where({ asset: pool }).
    select(knex.raw('sum(asset_e8) as gas')).
    whereRaw('gas_events.block_timestamp <= ?', timestamp).
    whereRaw('gas_events.block_timestamp > ?', 1598431212214856518).
    first();

  if (debug) console.log({ gas });

  const totalDepthWithoutGas = parseInt(totalstakes || 0) - parseInt(totalunstakes || 0) + parseInt(swapins || 0) - parseInt(swapouts || 0) + parseInt(feeassetamt || 0) - parseInt(swapfees || 0) - parseInt(unstakefees || 0) + parseInt(adds || 0);
  const totalDepth = totalDepthWithoutGas === 0 ? totalDepthWithoutGas : totalDepthWithoutGas - parseInt(gas || 0);
  if (debug) console.log('TOTAL DEPTH: ', totalDepth);

  return totalDepth;
}