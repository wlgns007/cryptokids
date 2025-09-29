import { ethers } from "ethers";
import fs from "fs";

const provider = new ethers.JsonRpcProvider("http://127.0.0.1:8545");
const PK = process.env.PK;
if (!PK || !PK.startsWith("0x") || PK.length !== 66) {
  throw new Error('Set PK first: $env:PK="0x<private_key_from_node_window>"');
}
const wallet = new ethers.Wallet(PK, provider);

const art = JSON.parse(
  fs.readFileSync("./artifacts/contracts/RewardSystem.sol/RewardSystem.json", "utf8")
);
const factory = new ethers.ContractFactory(art.abi, art.bytecode, wallet);

// pull the correct next nonce from the chain you’re on
const nonce = await provider.getTransactionCount(wallet.address);
const contract = await factory.deploy({ nonce });
await contract.waitForDeployment();
const addr = await contract.getAddress();
console.log("RewardSystem deployed at:", addr);

// merge into addresses.json
let addrs = {};
try { addrs = JSON.parse(fs.readFileSync("./addresses.json", "utf8")); } catch {}
addrs.RewardSystem = addr;
fs.writeFileSync("./addresses.json", JSON.stringify(addrs, null, 2));
console.log("addresses.json updated.");
