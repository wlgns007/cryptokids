// scripts/deploy.mjs
import hre from "hardhat";

async function main() {
  const { ethers } = hre;

  const PiggyBank = await ethers.getContractFactory("PiggyBank");
  const piggy = await PiggyBank.deploy();
  await piggy.waitForDeployment();
  console.log("PiggyBank deployed at:", await piggy.getAddress());
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
