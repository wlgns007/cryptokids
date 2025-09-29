// scripts/deploy_plain.mjs
import { ethers } from "ethers";
import fs from "fs";

// 1) RPC + signer (use Account #0 private key from your node window)
const provider = new ethers.JsonRpcProvider("http://127.0.0.1:8545");

// >>> REPLACE THIS with the Private Key of Account #0 shown in the hardhat node window <<<
const PRIVATE_KEY = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";
const wallet = new ethers.Wallet(PRIVATE_KEY, provider);

// 2) Load artifacts (adjust names if your files differ)
function loadArtifact(name) {
  const p = `./artifacts/contracts/${name}.sol/${name}.json`;
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

async function deployOne(name) {
  const { abi, bytecode } = loadArtifact(name);
  const factory = new ethers.ContractFactory(abi, bytecode, wallet);
  const contract = await factory.deploy();
  const receipt = await contract.deploymentTransaction().wait();
  const addr = await contract.getAddress();
  console.log(`${name} deployed at:`, addr, "| tx:", receipt.hash);
  return { name, addr };
}

function mergeWriteJSON(path, obj) {
  let data = {};
  if (fs.existsSync(path)) {
    try { data = JSON.parse(fs.readFileSync(path, "utf8")); } catch {}
  }
  fs.writeFileSync(path, JSON.stringify({ ...data, ...obj }, null, 2));
}

async function main() {
  // Deploy the simple ones we made
  const piggy = await deployOne("PiggyBank");
  const reward = await deployOne("RewardSystem");

  // Save addresses for later use by UI/scripts
  mergeWriteJSON("./addresses.json", {
    PiggyBank: piggy.addr,
    RewardSystem: reward.addr
  });
  console.log("addresses.json updated.");
}

main().catch((e) => { console.error(e); process.exitCode = 1; });
