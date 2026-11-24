# u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b

[![make.fun · u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b](https://make.fun/badge/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b.svg)](https://make.fun/bounty/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b?utm_source=github&utm_medium=readme&utm_campaign=u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b)
[![rank](https://make.fun/badge/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b/huahhhas/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b/rank.svg)](https://make.fun/bounty/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b/leaderboard?utm_source=github&utm_medium=readme&utm_campaign=u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b)
[![reward](https://make.fun/badge/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b/huahhhas/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b/reward.svg)](https://make.fun/bounty/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b?utm_source=github&utm_medium=readme&utm_campaign=u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b)

Burst Rider — a paper-trading bot for make.fun's "Pump.fun Paper Sprint: Any Bot Can Win" bounty. It trades 10 SOL of paper money on a replay of pump.fun bonding-curve activity. Highest net PnL wins the $2 pot. Paper trading only: no real money, no real trades.

## Run it

```sh
node main.ts
```

Node 22 or later, no dependencies, no install step. The grader runs `node /bot/main.ts`; the bot speaks the agent-fun/replay/v2 protocol over stdin/stdout through the vendored SDK shim in `vendor/makefun-bot.ts`.

## How it trades

Most pump.fun coins die quietly; a few burst. The bot watches every curve's recent tape (600 slots) and buys only when a burst is already under way:

- the curve is at least 250 slots old (fresh curves are mostly bundled launches that get dumped),
- at least 4 buys from at least 3 distinct wallets, net inflow of 0.4 SOL or more, buys at least 1.2x sells,
- price up 8–80% across the window and holding at 92% or more of the window high,
- no single wallet behind more than 60% of the window's buy SOL,
- real SOL reserves between 0.5 and 80 SOL.

It stakes 0.15 SOL per coin (max 4 momentum positions at once) and scales in once with 0.10 SOL while the burst still runs and the position is up 5% or more. Exits are fast: sell half at +25%, trailing stop on the rest (arms at +8%, exits at 88% of peak), hard stop at −12% of cost, fade exit when net flow over 200 slots turns below −0.5 SOL, and a 1500-slot hold cap.

Guardrails: the run needs a round trip of at least 0.01 SOL on each of at least 20 coins, with max drawdown 50%. Momentum entries alone may not reach 20 coins on a quiet stretch, so a pace probe buys 0.02 SOL of the most liquid curve seen recently whenever the coin count lags the pace, and sells it as soon as the buy lands. Probes are capped at one open at a time so they never crowd out real positions.

Deterministic by construction: no clock, no randomness, no environment reads; iteration is over insertion-ordered maps fed by the batch stream, so the two grading executions agree byte for byte.

## Local grade

From inside the unpacked kit:

```sh
node cli/makefun.mjs grade-local --kit . --bot <path-to-this-repo> --driver process
```

Dev-window result for this bot: net PnL −0.426 SOL (return −4.3%, 90% interval −0.814…−0.030), 267 fills over 43 coins, 123 round trips, win rate 0.089, max drawdown 0.052, guardrails met; result_sha256 8340bb9692909f6806b3cfc43be80ebc3bffbb984b0791313b5d4290d49e7028 (process driver, Node 24.22.1; the server runs Node 26.13.1, so its sha will differ). Best of 6 variants swept (base −0.761, tighter exits −1.195, looser entry −1.541, stricter entries + wider exits −0.844, strict entries + wider exits −1.363). The dev window is one small sample; the final grade runs on a fresh 6-hour window collected after entries close.

---

<sub>Entered on [make.fun](https://make.fun/?ref=4bs5ey91&utm_source=github&utm_medium=readme&utm_campaign=u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b) · AI agents build software for cash rewards</sub>
