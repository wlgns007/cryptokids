import { ethers } from "ethers";
import fs from "fs";

const provider = new ethers.JsonRpcProvider("http://127.0.0.1:8545");
const PK = process.env.PK;
if (!PK || !PK.startsWith("0x") || PK.length !== 66) {
  throw new Error('Set PK first: $env:PK="0x<private_key_from_node_window>"');
}
const signer = new ethers.Wallet(PK, provider);

const load = (n) =>
  JSON.parse(fs.readFileSync(`./artifacts/contracts/${n}.sol/${n}.json`, "utf8"));

const save = (obj) => {
  const path = "./addresses.json";
  let prev = {};
  if (fs.existsSync(path)) {
    try { prev = JSON.parse(fs.readFileSync(path, "utf8").replace(/^\uFEFF/, "")); } catch {}
  }
  fs.writeFileSync(path, JSON.stringify({ ...prev, ...obj }, null, 2));
};

const U = (n) => ethers.parseUnits(n, 18);

let nonce;
async function nextNonce() {
  if (nonce === undefined) nonce = await provider.getTransactionCount(await signer.getAddress());
  return nonce++;
}

async function deploy(name, ...args) {
  const art = load(name);
  const factory = new ethers.ContractFactory(art.abi, art.bytecode, signer);
  const c = await factory.deploy(...args, { nonce: await nextNonce() });
  await c.waitForDeployment();
  const addr = await c.getAddress();
  console.log(`${name} @ ${addr}`);
  return new ethers.Contract(addr, art.abi, signer);
}

async function main() {
  // 1) Deploy RT + VC
  const rt = await deploy("RewardToken");
  const vc = await deploy("BadgeVC");

  // 2) Kiosk wired to RT + VC
  const kiosk = await deploy("Kiosk", await rt.getAddress(), await vc.getAddress());

  // 3) Permissions + config (each uses next nonce)
  await (await rt.setMinter(await kiosk.getAddress(), true, { nonce: await nextNonce() })).wait();
  await (await vc.setRedeemer(await kiosk.getAddress(), true, { nonce: await nextNonce() })).wait();
  await (await kiosk.setBonus("EARLY_BIRD_7", U("10"), { nonce: await nextNonce() })).wait();

  // 4) Oya Shop (treasury = your address). Add item #1 price 6 RT
  const me = await signer.getAddress();
  const shop = await deploy("OyaShop", await rt.getAddress(), me);
  await (await shop.setItem(1, U("6"), true, { nonce: await nextNonce() })).wait();

  // 5) Save addresses
  save({
    RewardToken: await rt.getAddress(),
    BadgeVC:     await vc.getAddress(),
    Kiosk:       await kiosk.getAddress(),
    OyaShop:     await shop.getAddress()
  });
  console.log("addresses.json updated.");
}

main().catch((e)=>{ console.error(e); process.exitCode = 1; });
