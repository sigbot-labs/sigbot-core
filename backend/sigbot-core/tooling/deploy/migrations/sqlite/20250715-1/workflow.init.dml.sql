INSERT INTO t_workflow (name, flow_json) VALUES 
('ETH Arbitrage Strategy', '{
  "nodes": [
    {
      "id": "START",
      "type": "start",
      "position": {"x": 100, "y": 200},
      "data": {"name": "Start"},
      "inputs": [],
      "outputs": ["trigger"]
    },
    {
      "id": "twitter-feed-1", 
      "type": "twitter-feed",
      "position": {"x": 300, "y": 200},
      "data": {
        "name": "Twitter Feed",
        "type": "twitter-feed",
        "accounts": ["elonmusk", "VitalikButerin"],
        "keywords": ["ETH", "Ethereum"],
        "enableWebSocket": false,
        "pollingInterval": 5,
        "cacheRetention": 30
      },
      "inputs": ["trigger"],
      "outputs": ["data"]
    },
    {
      "id": "uniswap-feed-1",
      "type": "uniswap-feed",
      "position": {"x": 500, "y": 200},
      "data": {
        "name": "Uniswap Feed",
        "type": "uniswap-feed",
        "rpcEndpoint": "https://eth-mainnet.alchemyapi.io/v2/your-api-key",
        "poolAddress": "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640",
        "pollingInterval": 5
      },
      "inputs": ["data"],
      "outputs": ["data"]
    },
    {
      "id": "END",
      "type": "end", 
      "position": {"x": 700, "y": 200},
      "data": {"name": "End"},
      "inputs": ["result"],
      "outputs": []
    }
  ],
  "connections": [
    {
      "id": "conn-1",
      "source": "START",
      "target": "twitter-feed-1", 
      "sourceOutput": "trigger",
      "targetInput": "trigger"
    },
    {
      "id": "conn-2",
      "source": "twitter-feed-1",
      "target": "uniswap-feed-1",
      "sourceOutput": "data",
      "targetInput": "data"
    }
  ],
  "status": "draft"
}')
ON CONFLICT DO NOTHING;