const date = require('date-and-time');
const mysql = require('mysql');
const BN = require('bn.js');

require('dotenv').config();

const connection = mysql.createConnection({
	host: process.env.DB_HOST,
	user: process.env.DB_USER,
	password: process.env.DB_PASSWORD,
	database: process.env.DB_DATABASE,
});

const tableName = process.env.TABLE_NAME;
const nftTableName = process.env.NFT_TABLE_NAME;

const addDB = async (data) => {	
	try {
		// Check if duplicated transaction hash
		const sql1 = `select * from ${tableName} where chainId = ${data.chainId} and transactionHash = '${data.transactionHash}'`;
		return connection.query(sql1, async function(error, result, fields) {
			if(result.length > 0) {
				// console.log(data.transactionHash + ' exist in db...');
				return false;
			} else {
				const sql = `INSERT into ${tableName}(team, referee, referer, createAt, transactionHash, blockHeight, chainId) values (${process.env.TEAM},'${data.referee}', '${data.referer}', ${data.timestamp / 1000},'${data.transactionHash}', ${data.blockHeight}, ${data.chainId})`;
				const promise = new Promise((resolve, reject) => {
					connection.query(
						sql,
						function (error, d, fields) {
							if(error) return reject(error);
							else {
								const dt = date.format(new Date(data.timestamp), 'YYYY-MM-DD HH:mm:ss');
								console.log(`=> ${dt} referee: ${data.referee}, referer: ${data.referer} hash ${data.transactionHash}`);
								return resolve(data);
							}
						}
					)
				})
				return promise;	
			}
		})
	} catch (error) {
		console.error(error);
	}
}

const updateStatusDB = async (data) => {
	try {
		const sql1 = `select * from ${tableName} where chainId = ${data.chainId} and referee = '${data.referee}' and funded = 0`;
		return connection.query(sql1, function (error, result, fields) {
			if(result.length === 0) {
				return false;
			} else {
				const sql = `update ${tableName} set funded = 1, rate = ${data.rate}, amount = ${data.amount} where chainId = '${data.chainId}' and referee = '${data.referee}'`;
				const promise = new Promise((resolve, reject) => {
					connection.query(
						sql,
						function (error, d, fields) {
							if(error) return reject(error);
							else {
								const dt = date.format(new Date(data.timestamp), 'YYYY-MM-DD HH:mm:ss');
								console.log(`=> ${dt} referee: ${data.referee} funded, hash ${data.transactionHash}`);
								return resolve(data);
							}
						}
					)
				})
				return promise;
			}
		})
	} catch (error) {
		console.log(error);
	}
}

/**
 * While mint a new NFT token
 * @param {*} mintNftData 
 * @returns 
 */
 const addNFTDB = async (mintNftData, crossFromChainId, crossFromNFTAddress) => {
	try {
		// Check if duplicated transaction hash
		// console.log('addNFTDB', mintNftData.tokenId)
		const sql1 = `select * from ${nftTableName} where 
									chainId = ${mintNftData.chainId} and nftAddress = '${mintNftData.contractAddress}' and tokenId = ${mintNftData.tokenId}`;
		return connection.query(sql1, async function(error, result, fields) {
			if(result !== undefined && result.length > 0) {
				if(result[0].owner.toLowerCase() !== mintNftData.transferTo.toLowerCase()) {
					// Update owner
					const sql = `update ${nftTableName} set owner = '${mintNftData.transferTo}' 
											 where chainId = ${mintNftData.chainId} and nftAddress = '${mintNftData.contractAddress}' and tokenId = ${mintNftData.tokenId}`;
					const promise = new Promise((resolve, reject) => {
						connection.query(
							sql,
							function (error2, data2, fields) {
								if(error2) return reject(error2);
								else {
									console.log(`=> ${mintNftData.blockNumber} Transfer #${mintNftData.tokenId} to ${mintNftData.transferTo} hash ${mintNftData.transactionHash}`);
									return resolve(data2);
								}
							}
						)
					})
					return promise;
				} else {
					return false;
				}
			} else {
				const sql = `INSERT into ${nftTableName}(chainId, nftAddress, tokenId, owner, createAt) 
										 values ('${mintNftData.chainId}', '${mintNftData.contractAddress}', '${mintNftData.tokenId}', '${mintNftData.transferTo}', unix_timestamp())`;
				const promise = new Promise((resolve, reject) => {
					connection.query(
						sql,
						function (error, data, fields) {
							if(error || data === undefined) return reject(error);
							else {
								console.log(`=> ${mintNftData.blockNumber} Mint tokenId #${mintNftData.tokenId} hash ${mintNftData.transactionHash}`);
							}
						}
					)
				})
				return promise;	
			}
		})
	} catch (error) {
		console.error(error);
	}
}

/**
 * While transfer NFT token
 * @param {*} buyData 
 * @returns 
 */
const updateOwnerNFTDB = async (mintNftData) => {
	try {
		const sql = `select * from ${nftTableName} where chainId = ${mintNftData.chainId} and nftAddress = '${mintNftData.contractAddress}' and tokenId = ${mintNftData.tokenId}`;
		return connection.query(
			sql, 
			async function(error, data1, fields) {
				if(data1 === undefined || data1.length === 0) return false;
				else {
					const sql = `update ${nftTableName} set owner = '${mintNftData.transferTo}' where chainId = ${mintNftData.chainId} and nftAddress = '${mintNftData.contractAddress}' and tokenId = ${mintNftData.tokenId}`;
					const promise = new Promise((resolve, reject) => {
						connection.query(
							sql,
							function (error, data2, fields) {
								if(error) return reject(error);
								else {
									// const dt = date.format(new Date(mintNftData.dateTime * 1000), 'YYYY-MM-DD HH:mm:ss');
									console.log(`=> ${mintNftData.blockNumber} Transfer #${mintNftData.tokenId} to ${mintNftData.transferTo} hash ${mintNftData.transactionHash}`);
									return resolve(data2);
								}
							}
						)
					})
					return promise;
				}
		})
	} catch (error) {
		console.error(error);
	}
}

/**
 * Cause the database duplicated write, do the following sql and delete the duplicated records
 */
const deleteDuplicateRowsByField = async (field) => {
	try {
		const sql = `delete orders from orders inner join (select max(id) as lastId, ${field} from orders group by ${field} having count(*) > 1) duplic on duplic.${field} = orders.${field} where orders.id < duplic.lastId`;
		return connection.query(sql, function(error, data, fields) {
			console.log(`=> Delete duplicated ${field} rows: ${data.affectedRows}`);
			return data.affectedRows;
		})	
	} catch(error) {
		console.error(error);
	}
}

module.exports = {
	addDB,
	updateStatusDB,
	deleteDuplicateRowsByField,
	addNFTDB,
	updateOwnerNFTDB
}