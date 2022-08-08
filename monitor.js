const Web3 = require('web3');
const {
	parseData, 
} = require('./src/parseLogs');
const {
	addDB,
} = require('./src/db');

// const {
// 	getNftType,
// } = require('./src/nftType');

// BSC Mainnet
// const wss = 'wss://ws-nd-475-177-088.p2pify.com/d86115e778aa9c1e71b5403a89b73fa9';
// rinkeby testnet
const wss = 'wss://rinkeby.infura.io/ws/v3/3446259cb0e74d68b614f9a10328a368';
// AddReferer event topic
const referalTopic = "0x908e62ce7b06deb6a3703a76690e5d7527b81cc37455286dd54f9126152bcf4f";

const web3 = new Web3(new Web3.providers.WebsocketProvider(wss, {
	clientConfig: {
			keepalive: true,
			keepaliveInterval: 60000	// milliseconds
	},
	// Enable auto reconnection
	reconnect: {
			auto: true,
			delay: 5000, // ms
			maxAttempts: 10,
			onTimeout: false
	}
}));

console.log('NUSIC server start running...');
console.log("Web3 connected...");

const subscription_referals = () => {
	web3.eth.subscribe('logs', {
		topics: [referalTopic]
	}, function(error, result){
	})
	.on("connected", function(subscriptionId){
		console.log('subscription_sell Id:' + subscriptionId);
	})
	.on("data", async function(log){
		await _parseData(log);
	})
	.on("error", function(error) {
		console.log(error);
		subscription_sell();
	})
}

const _parseData = async (log) => {
	let data = parseData(web3, log);
	await addDB(data);
}

subscription_referals();
