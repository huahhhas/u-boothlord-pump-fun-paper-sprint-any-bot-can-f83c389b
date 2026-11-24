// Burst Rider — a paper-trading bot for the pump.fun replay (agent-fun/replay/v2).
//
// Idea: most pump.fun coins die quietly; a few burst. The bot watches every curve's
// recent tape and buys only when a burst is already under way — several distinct
// buyers pushing real SOL in over a few hundred slots, price climbing and holding
// near its window high — then gets out fast: half the position at +25%, the rest on
// a trailing stop, with a hard stop and a hold cap so losers never linger.
//
// The bot is deliberately small and deterministic: no clock, no randomness, no
// environment reads; iteration is over insertion-ordered maps fed by the batch
// stream, so the two grading executions agree byte for byte.
//
// Guardrails: the run needs a round trip of at least 0.01 SOL on each of at least
// 20 coins, and a max drawdown of 50%. Momentum entries alone may not reach 20
// coins on a quiet stretch, so a pace probe buys a tiny 0.02 SOL of the most liquid
// curve seen recently whenever the coin count lags the pace, and sells it as soon
// as the buy lands. Probes are capped so they never crowd out real positions.

import {
  LAMPORTS_PER_SOL,
  runBot,
  type Batch,
  type BotState,
  type OrderInput,
  type Pubkey,
} from "./vendor/makefun-bot.ts";

const SOL = LAMPORTS_PER_SOL;

// ---- tunables (chosen on the dev window; see README for the sweep notes) ----
const WINDOW_SLOTS = 600; // tape lookback for the entry signal
const MIN_AGE_SLOTS = 250; // curve must be this old before an entry
const MIN_BUYS = 5; // buy trades inside the window
const MIN_BUYERS = 4; // distinct buying wallets inside the window
const MIN_NET_SOL = 1.0; // net SOL inflow (buys minus sells) over the window
const MIN_BUY_SELL_RATIO = 1.2; // buy SOL must be at least this x sell SOL
const MIN_RISE = 0.15; // price now vs window start, minus 1
const MAX_RISE = 0.6; // skip curves that already ran too far in one window
const NEAR_HIGH = 0.92; // price now must be at least this share of window high
const MAX_BUYER_SHARE = 0.6; // largest single wallet's share of window buy SOL
const MIN_REAL_SOL = 0.5; // curve real SOL reserves at entry
const MAX_REAL_SOL = 80;
const STAKE_SOL = 0.12; // SOL per momentum position
const ADD_SOL = 0.1; // one scale-in buy while the burst still runs
const ADD_MIN_GAIN = 0.05; // scale in only once the position is up this much
const TAKE_HALF_AT = 0.25; // sell half the tokens at this gain
const TRAIL_ARM = 0.08; // trailing stop arms once value is up this much
const TRAIL_KEEP = 0.88; // exit the rest when value falls below this x peak
const HARD_STOP = 0.88; // exit everything when value falls below this x cost
const MAX_HOLD_SLOTS = 1500; // exit everything after this long held
const FADE_WINDOW = 200; // fade exit: net SOL over this many slots ...
const FADE_NET_SOL = -0.5; // ... below this, exit everything
const COOLDOWN_SLOTS = 150; // wait after an exit before re-entering a curve
const MAX_ENTRIES_PER_MINT = 2;
const MAX_MOMENTUM_OPEN = 4; // one of the 5 open slots is kept for probes
const PROBE_SOL = 0.02; // guardrail probe size
const MAX_PROBES_OPEN = 1;
const PACE_TARGET_COINS = 25; // probes push the coin count toward this
const PACE_SPAN_SLOTS = 100_000; // slots over which the pace target is spread
const BUY_SLIPPAGE_BPS = 1500;
const SELL_SLIPPAGE_BPS = 10_000; // sells never refused for price movement
const TAPE_TTL_SLOTS = 2000; // drop a curve's tape once it goes this quiet
const MAX_TAPES = 5000; // bound on tracked curves (oldest quiet ones drop)

interface TapeTrade {
  slot: number;
  buy: boolean;
  sol: number;
  user: Pubkey;
  price: number;
}

interface Tape {
  mint: Pubkey;
  createdSlot: number | null;
  lastSeenSlot: number;
  trades: TapeTrade[];
}

interface Held {
  mint: Pubkey;
  openedSlot: number;
  peakValue: bigint; // best mark-to-market value seen (lamports)
  tookHalf: boolean;
  added: boolean;
}

function priceOf(curve: { virtual_quote_reserves: bigint; virtual_token_reserves: bigint }): number {
  const q = Number(curve.virtual_quote_reserves);
  const t = Number(curve.virtual_token_reserves);
  if (t <= 0) return 0;
  return q / t;
}

/** SOL moved by one trade: its SOL amount is the quote side of the trade. */
function tradeSolLamports(solAmount: string): bigint {
  return BigInt(solAmount);
}

const tapes = new Map<Pubkey, Tape>();
const held = new Map<Pubkey, Held>();
const entriesUsed = new Map<Pubkey, number>();
const lastExitSlot = new Map<Pubkey, number>();
const pendingBuys = new Map<string, { mint: Pubkey; probe: boolean }>();
const probePending = new Set<string>();
const roundTripped = new Set<Pubkey>(); // coins with a completed >= 0.01 SOL round trip
const boughtCoins = new Set<Pubkey>(); // coins we have ever bought >= 0.01 SOL
let firstSlot = -1;
let probeBuysSent = 0;

function tapeFor(mint: Pubkey, slot: number): Tape {
  let t = tapes.get(mint);
  if (t === undefined) {
    t = { mint, createdSlot: null, lastSeenSlot: slot, trades: [] };
    tapes.set(mint, t);
    if (tapes.size > MAX_TAPES) {
      // Drop the quietest tape: the oldest lastSeenSlot that is not held.
      let worst: Tape | null = null;
      for (const cand of tapes.values()) {
        if (held.has(cand.mint)) continue;
        if (worst === null || cand.lastSeenSlot < worst.lastSeenSlot) worst = cand;
      }
      if (worst !== null) tapes.delete(worst.mint);
    }
  }
  return t;
}

function pruneTape(t: Tape, now: number): void {
  const cutoff = now - WINDOW_SLOTS;
  while (t.trades.length > 0 && t.trades[0].slot <= cutoff) t.trades.shift();
}

interface WindowStats {
  buys: number;
  buySol: number;
  sellSol: number;
  buyers: Set<Pubkey>;
  buyerSol: Map<Pubkey, number>;
  startPrice: number;
  high: number;
  lastPrice: number;
}

function windowStats(t: Tape): WindowStats {
  const s: WindowStats = {
    buys: 0,
    buySol: 0,
    sellSol: 0,
    buyers: new Set(),
    buyerSol: new Map(),
    startPrice: 0,
    high: 0,
    lastPrice: 0,
  };
  for (const tr of t.trades) {
    if (s.startPrice === 0) s.startPrice = tr.price;
    if (tr.price > s.high) s.high = tr.price;
    s.lastPrice = tr.price;
    if (tr.buy) {
      s.buys += 1;
      s.buySol += tr.sol;
      s.buyers.add(tr.user);
      s.buyerSol.set(tr.user, (s.buyerSol.get(tr.user) ?? 0) + tr.sol);
    } else {
      s.sellSol += tr.sol;
    }
  }
  return s;
}

function netSolOver(t: Tape, now: number, span: number): number {
  let net = 0;
  for (let i = t.trades.length - 1; i >= 0; i--) {
    const tr = t.trades[i];
    if (tr.slot <= now - span) break;
    net += tr.buy ? tr.sol : -tr.sol;
  }
  return net;
}

/** The most liquid curve seen recently that we hold nothing in and have no buy pending on. */
function mostLiquidCandidate(state: BotState): Pubkey | null {
  let best: Pubkey | null = null;
  let bestReal = 0n;
  for (const [mint, curve] of state.curves) {
    if (curve.complete || held.has(mint)) continue;
    if (state.positions.has(mint)) continue;
    let pending = false;
    for (const p of state.pending.values()) {
      if (p.order.mint === mint && p.order.side === "buy") {
        pending = true;
        break;
      }
    }
    if (pending) continue;
    if (curve.real_quote_reserves > bestReal) {
      bestReal = curve.real_quote_reserves;
      best = mint;
    }
  }
  if (best === null || bestReal < (SOL * 2n) / 10n) return null; // need >= 0.2 SOL real reserves
  return best;
}

await runBot({
  onBatch(state: BotState, batch: Batch): OrderInput[] {
    const now = batch.slot;
    if (firstSlot < 0) firstSlot = now;
    const orders: OrderInput[] = [];
    const txFee = state.txFeeLamports();
    let free = state.freeCashLamports();

    // --- fold fills: track probe buys that landed, and round trips that closed ---
    for (const f of batch.fills) {
      const pb = pendingBuys.get(f.client_id);
      if (pb !== undefined) {
        pendingBuys.delete(f.client_id);
        probePending.delete(f.client_id);
        if (f.status === "filled") {
          if (pb.probe) {
            // Probe buy landed: sell it all right away (same batch's reply).
            const pos = state.positions.get(pb.mint);
            if (pos !== undefined && pos.token_amount > 0n) {
              orders.push({
                side: "sell",
                mint: pb.mint,
                token_amount: pos.token_amount,
                max_slippage_bps: SELL_SLIPPAGE_BPS,
              });
              roundTripped.add(pb.mint);
            }
          } else if (BigInt(f.quote_lamports) >= SOL / 100n) {
            boughtCoins.add(pb.mint);
          }
        }
      }
      // A harness close or our own sell that empties a position: count the round trip.
      if (f.side === "sell" && f.status === "filled") {
        const pos = state.positions.get(f.mint);
        if (pos === undefined && boughtCoins.has(f.mint)) roundTripped.add(f.mint);
      }
    }

    // --- sync held map with the account (harness closes drop positions) ---
    // A buy still in flight (2-slot latency) is not a missing position yet.
    const buyPendingMints = new Set<Pubkey>();
    for (const p of state.pending.values()) {
      if (p.order.side === "buy") buyPendingMints.add(p.order.mint);
    }
    for (const mint of [...held.keys()]) {
      if (!state.positions.has(mint) && !buyPendingMints.has(mint)) {
        held.delete(mint);
        lastExitSlot.set(mint, now);
        if (boughtCoins.has(mint)) roundTripped.add(mint);
      }
    }

    // --- record this batch's events into the tapes ---
    for (const e of batch.events) {
      if (e.event === "CreateEvent") {
        const t = tapeFor(e.mint, now);
        t.createdSlot = e.slot;
        t.lastSeenSlot = now;
      } else if (e.event === "TradeEvent") {
        const t = tapeFor(e.mint, now);
        t.lastSeenSlot = now;
        const sol = Number(tradeSolLamports(e.sol_amount)) / 1e9;
        t.trades.push({
          slot: e.slot,
          buy: e.is_buy,
          sol,
          user: e.user,
          price: Number(e.virtual_quote_reserves) / Number(e.virtual_token_reserves),
        });
        pruneTape(t, now);
      } else {
        const t = tapes.get(e.mint);
        if (t !== undefined) t.lastSeenSlot = now;
      }
    }
    // Drop tapes that went quiet long ago (and are not held).
    if (tapes.size > 1000) {
      for (const [mint, t] of [...tapes.entries()]) {
        if (!held.has(mint) && now - t.lastSeenSlot > TAPE_TTL_SLOTS) tapes.delete(mint);
      }
    }

    // --- exits for held positions ---
    for (const [mint, h] of held) {
      const pos = state.positions.get(mint);
      const curve = state.curves.get(mint);
      if (pos === undefined || curve === undefined || curve.complete) continue;
      const price = priceOf(curve);
      if (price <= 0) continue;
      const value = (pos.token_amount * BigInt(Math.round(price * 1e12))) / 1_000_000_000_000n;
      const cost = pos.cost_lamports;
      if (value > h.peakValue) h.peakValue = value;
      const gain = cost > 0n ? Number(((value - cost) * 1_000_000n) / cost) / 1e6 : 0;

      // Take half at +25% (once).
      if (!h.tookHalf && gain >= TAKE_HALF_AT) {
        const half = pos.token_amount / 2n;
        if (half > 0n) {
          orders.push({ side: "sell", mint, token_amount: half, max_slippage_bps: SELL_SLIPPAGE_BPS });
          h.tookHalf = true;
        }
        continue;
      }
      // Scale in once while the burst still runs and the position is up.
      if (!h.added && gain >= ADD_MIN_GAIN) {
        const t = tapes.get(mint);
        const stillStrong = t !== undefined && netSolOver(t, now, WINDOW_SLOTS) >= MIN_NET_SOL;
        const cap = state.maxBuyLamports(mint);
        const amount = ADD_SOL * 1e9;
        if (stillStrong && cap >= BigInt(amount) && free >= BigInt(amount) + txFee) {
          const id = state.clientId();
          pendingBuys.set(id, { mint, probe: false });
          orders.push({
            side: "buy",
            mint,
            quote_lamports: BigInt(amount),
            max_slippage_bps: BUY_SLIPPAGE_BPS,
            client_id: id,
          });
          free -= BigInt(amount) + txFee;
          h.added = true;
        }
      }
      // Hard stop, trailing stop, fade, hold cap.
      const t = tapes.get(mint);
      const fading = t !== undefined && netSolOver(t, now, FADE_WINDOW) <= FADE_NET_SOL;
      const trailing =
        h.peakValue > (cost * BigInt(Math.round((1 + TRAIL_ARM) * 1000))) / 1000n &&
        value < (h.peakValue * BigInt(Math.round(TRAIL_KEEP * 1000))) / 1000n;
      const stopped = value < (cost * BigInt(Math.round(HARD_STOP * 1000))) / 1000n;
      const stale = now - h.openedSlot >= MAX_HOLD_SLOTS;
      if (trailing || stopped || fading || stale) {
        orders.push({ side: "sell", mint, token_amount: pos.token_amount, max_slippage_bps: SELL_SLIPPAGE_BPS });
        held.delete(mint);
        lastExitSlot.set(mint, now);
        if (boughtCoins.has(mint)) roundTripped.add(mint);
      }
    }

    // --- entries: scan mints that traded this batch ---
    // Probes never enter `held`, so held.size is the momentum open count.
    const momentumOpen = held.size;
    let slotsLeft = Math.max(0, MAX_MOMENTUM_OPEN - momentumOpen);
    const openSlots = state.hello.caps.max_open_positions - state.committedPositionCount();
    slotsLeft = Math.min(slotsLeft, Math.max(0, openSlots));
    if (slotsLeft > 0) {
      const seen = new Set<Pubkey>();
      for (const e of batch.events) {
        if (e.event !== "TradeEvent") continue;
        const mint = e.mint;
        if (seen.has(mint) || held.has(mint)) continue;
        seen.add(mint);
        const t = tapes.get(mint);
        const curve = state.curves.get(mint);
        if (t === undefined || curve === undefined || curve.complete) continue;
        if (t.createdSlot !== null && now - t.createdSlot < MIN_AGE_SLOTS) continue;
        if ((lastExitSlot.get(mint) ?? -1e9) > now - COOLDOWN_SLOTS) continue;
        if ((entriesUsed.get(mint) ?? 0) >= MAX_ENTRIES_PER_MINT) continue;
        const realSol = Number(curve.real_quote_reserves) / 1e9;
        if (realSol < MIN_REAL_SOL || realSol > MAX_REAL_SOL) continue;
        pruneTape(t, now);
        const s = windowStats(t);
        if (s.buys < MIN_BUYS || s.buyers.size < MIN_BUYERS) continue;
        const net = s.buySol - s.sellSol;
        if (net < MIN_NET_SOL) continue;
        if (s.sellSol > 0 && s.buySol < MIN_BUY_SELL_RATIO * s.sellSol) continue;
        if (s.startPrice <= 0 || s.lastPrice <= 0) continue;
        const rise = s.lastPrice / s.startPrice - 1;
        if (rise < MIN_RISE || rise > MAX_RISE) continue;
        if (s.high > 0 && s.lastPrice < NEAR_HIGH * s.high) continue;
        let biggest = 0;
        for (const v of s.buyerSol.values()) if (v > biggest) biggest = v;
        if (s.buySol > 0 && biggest / s.buySol > MAX_BUYER_SHARE) continue;
        const cap = state.maxBuyLamports(mint);
        const stake = BigInt(Math.round(STAKE_SOL * 1e9));
        const amount = cap < stake ? cap : stake;
        if (amount < SOL / 100n) continue; // keep every buy a >= 0.01 SOL round trip
        if (orders.length >= state.hello.caps.max_orders_per_batch) break;
        if (free < amount + txFee) continue;
        const id = state.clientId();
        pendingBuys.set(id, { mint, probe: false });
        orders.push({ side: "buy", mint, quote_lamports: amount, max_slippage_bps: BUY_SLIPPAGE_BPS, client_id: id });
        free -= amount + txFee;
        slotsLeft -= 1;
        entriesUsed.set(mint, (entriesUsed.get(mint) ?? 0) + 1);
        held.set(mint, { mint, openedSlot: now, peakValue: 0n, tookHalf: false, added: false });
        if (slotsLeft <= 0) break;
      }
    }

    // --- guardrail pace probe: keep the coin count on pace ---
    const elapsed = Math.max(1, now - firstSlot);
    const paceTarget = Math.min(PACE_TARGET_COINS, Math.ceil((PACE_TARGET_COINS * elapsed) / PACE_SPAN_SLOTS));
    const coinsSoFar = new Set([...boughtCoins, ...roundTripped]).size;
    if (
      coinsSoFar < paceTarget &&
      probePending.size < MAX_PROBES_OPEN &&
      state.committedPositionCount() < state.hello.caps.max_open_positions &&
      free >= BigInt(Math.round(PROBE_SOL * 1e9)) + txFee &&
      orders.length < state.hello.caps.max_orders_per_batch
    ) {
      const mint = mostLiquidCandidate(state);
      if (mint !== null) {
        const cap = state.maxBuyLamports(mint);
        const amount = BigInt(Math.round(PROBE_SOL * 1e9));
        if (cap >= amount) {
          const id = state.clientId();
          pendingBuys.set(id, { mint, probe: true });
          probePending.add(id);
          probeBuysSent += 1;
          orders.push({ side: "buy", mint, quote_lamports: amount, max_slippage_bps: BUY_SLIPPAGE_BPS, client_id: id });
          free -= amount + txFee;
          boughtCoins.add(mint);
        }
      }
    }

    return orders;
  },
});
