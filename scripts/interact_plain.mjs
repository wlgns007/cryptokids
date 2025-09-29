import { ethers } from "ethers";
import fs from "fs";

const provider = new ethers.JsonRpcProvider("http://127.0.0.1:8545");
const PK = process.env.PK;
if (!PK || !PK.startsWith("0x") || PK.length !== 66) {
  throw new Error('Set PK first: $env:PK="0x<private_key_from_node_window>"');
}
const wallet = new ethers.Wallet(PK, provider);

// --- helpers ---
function cleanJSON(path) {
  // strip BOM + trim just in case the file was saved with a BOM/extra space
  const raw = fs.readFileSync(path, "utf8").replace(/^\uFEFF/, "").trim();
  return JSON.parse(raw);
}
function norm(addr) {
  // turn into checksummed 0x… string and strip stray whitespace/newlines
  return ethers.getAddress(String(addr).trim());
}
function loadArtifact(name) {
  return JSON.parse(
    fs.readFileSync(`./artifacts/contracts/${name}.sol/${name}.json`, "utf8")
  );
}

// --- main ---
async function main() {
  const addrs = cleanJSON("./addresses.json");

  // PiggyBank
  const piggy = new ethers.Contract(
    norm(addrs.PiggyBank),
    loadArtifact("PiggyBank").abi,
    wallet
  );

  console.log("Depositing 0.01 ETH to PiggyBank...");
  const tx1 = await piggy.deposit({ value: ethers.parseEther("0.01") });
  await tx1.wait();
  const me = await wallet.getAddress();                 // v6-safe
  const bal = await piggy.balances(me);
  console.log("PiggyBank balance for me:", ethers.formatEther(bal), "ETH");

  // RewardSystem
  const reward = new ethers.Contract(
    norm(addrs.RewardSystem),
    loadArtifact("RewardSystem").abi,
    wallet
  );

  console.log("Giving 50 points...");
  const tx2 = await reward.givePoints(me, 50);          // pass normalized address
  await tx2.wait();
  const pts = await reward.points(me);
  console.log("My points now:", pts.toString());
}

main().catch((e) => { console.error(e); process.exitCode = 1; });
