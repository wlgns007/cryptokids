// scripts/deploy.js
const hre = require("hardhat");

async function main() {
  const { ethers } = hre;

  const PiggyBank = await ethers.getContractFactory("PiggyBank");
  const piggy = await PiggyBank.deploy();
  await piggy.deployed(); // CJS plugin exposes .deployed()
  console.log("PiggyBank deployed at:", piggy.address);
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
