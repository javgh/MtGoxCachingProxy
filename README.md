This tool will open a connection to the Mt.Gox websocket API (
wss://websocket.mtgox.com/mtgox ), subscribe to a few additional channels
(ticker.BTCEUR and depth.BTCEUR) and forward all communication to a client, that
connects locally on port 10508. It will furthermore maintain a history of
messages and send them out in an initial burst, when the client first connects.

The idea behind this tool is, that a client using the Mt.Gox websocket data feed
can be restarted frequently (for example during development) and quickly get up
to speed again. Furthermore the tool will detect connection problems and
reconnect to Mt.Gox automatically.
