# WOO X BTC Open Interest Stream

A Node.js application that streams real-time BTC open interest data from the WOO X WebSocket API.

## Features

- Real-time open interest updates for BTC symbols
- Automatic reconnection on disconnect
- Ping-Pong mechanism for connection verification
- Graceful shutdown handling

## Installation

```bash
npm install
```

## Usage

```bash
npm start
```

## Configuration

Edit `index.js` to add or remove BTC symbols:

```javascript
const BTC_SYMBOLS = [
  'PERP_BTC_USDT',
  'SPOT_BTC_USDT',
  'PERP_BTC_USDC',
  // Add more BTC symbols
];
```

## WebSocket Details

- **Public URL**: `wss://wss.woox.io/v3/public`
- **Topic Format**: `openinterest@{symbol}`
- **Update Interval**: Every 1 second on change, 10 seconds force update

## Sample Output

```
--- Open Interest Update ---
Topic: openinterest@PERP_BTC_USDT
Symbol: PERP_BTC_USDT
Open Interest: 936.37
Last Update: 2024-04-19T10:30:00.000Z
Received at: 2024-04-19T10:30:01.552Z
---------------------------
```

## Limits

- Max 100 connections per IP per 5 minutes
- Max 100 active connections per IP
- Max 100 topics per connection
- Connection valid for 24 hours