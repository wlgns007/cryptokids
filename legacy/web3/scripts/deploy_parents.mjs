import { ethers } from "ethers";
import fs from "fs";

const provider = new ethers.JsonRpcProvider("http://127.0.0.1:8545");
const PK = process.env.PK;
if (!PK || !PK.startsWith("0x") || PK.length !== 66) throw new Error("Set PK");

const signer = new ethers.Wallet(PK, provider);
const load = (n) => JSON.parse(fs.readFileSync(`./artifacts/contracts/${n}.sol/${n}.json`, "utf8"));
const U = (n) => ethers.parseUnits(n, 18);

let nonce;
async function nextNonce(){ if(nonce===undefined) nonce=await provider.getTransactionCount(await signer.getAddress()); return nonce++; }

async function deploy(name, ...args){
  const art = load(name);
  const c = await new ethers.ContractFactory(art.abi, art.bytecode, signer).deploy(...args, { nonce: await nextNonce() });
  await c.waitForDeployment();
  const addr = await c.getAddress();
  console.log(`${name} @ ${addr}`);
  return new ethers.Contract(addr, art.abi, signer);
}

const save = (obj) => {
  let prev={}; const p="./addresses.json";
  if (fs.existsSync(p)) try{ prev=JSON.parse(fs.readFileSync(p,"utf8").replace(/^\uFEFF/, "")); }catch{}
  fs.writeFileSync(p, JSON.stringify({ ...prev, ...obj }, null, 2));
};

const main = async () => {
  // if you already deployed RewardToken/BadgeVC/Kiosk, reuse their addresses:
  const addrs = JSON.parse(fs.readFileSync("./addresses.json","utf8").replace(/^\uFEFF/,""));
  const rtAddr = addrs.RewardToken;
  const me = await signer.getAddress();

  // Deploy ParentsShop and set item #1 price = 6 RT
  const shop = await deploy("ParentsShop", rtAddr, me);
  await (await shop.setItem(1, U("6"), true, { nonce: await nextNonce() })).wait();

  save({ ParentsShop: await shop.getAddress() });
  console.log("addresses.json updated.");
};
main().catch(e=>{ console.error(e); process.exitCode=1; });
