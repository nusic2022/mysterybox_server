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
}