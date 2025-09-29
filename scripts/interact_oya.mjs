import { ethers, NonceManager } from "ethers";
import fs from "fs";

const provider = new ethers.JsonRpcProvider("http://127.0.0.1:8545");
const PK = process.env.PK;
if (!PK || PK.length !== 66 || !PK.startsWith("0x")) throw new Error("Set PK first");

// wrap the wallet so nonces auto-increment across calls
const base = new ethers.Wallet(PK, provider);
const me   = new NonceManager(base);

const clean = (p) => JSON.parse(fs.readFileSync(p, "utf8").replace(/^\uFEFF/, "").trim());
const addrs = clean("./addresses.json");
const load  = (n) => JSON.parse(fs.readFileSync(`./artifacts/contracts/${n}.sol/${n}.json`, "utf8"));
const A     = (n) => ethers.parseUnits(n, 18);
const norm  = (a) => ethers.getAddress(String(a).trim());

const rt    = new ethers.Contract(norm(addrs.RewardToken), load("RewardToken").abi, me);
const vc    = new ethers.Contract(norm(addrs.BadgeVC),     load("BadgeVC").abi,     me);
const kiosk = new ethers.Contract(norm(addrs.Kiosk),       load("Kiosk").abi,       me);
const shop  = new ethers.Contract(norm(addrs.OyaShop),     load("OyaShop").abi,     me);

const main = async () => {
  const addr = await me.getAddress();

  console.log("Minting 12 RT to kid...");
  await (await rt.mint(addr, A("12"))).wait();
  console.log("RT balance:", ethers.formatUnits(await rt.balanceOf(addr), 18));

  console.log("Approving OyaShop for 6 RT...");
  await (await rt.approve(norm(addrs.OyaShop), A("6"))).wait();

  console.log("Buying item #1...");
  await (await shop.buy(1, 1)).wait();
  console.log("RT after purchase:", ethers.formatUnits(await rt.balanceOf(addr), 18));

  console.log('Issuing VC "EARLY_BIRD_7"...');
  await (await vc.issue(addr, "EARLY_BIRD_7")).wait();

  console.log("Redeeming VC for bonus RT...");
  await (await kiosk.redeem("EARLY_BIRD_7")).wait();
  console.log("RT after redeem:", ethers.formatUnits(await rt.balanceOf(addr), 18));
};

main().catch((e)=>{ console.error(e); process.exitCode = 1; });
