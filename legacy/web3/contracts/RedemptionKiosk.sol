// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

import "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import "@openzeppelin/contracts/access/Ownable.sol";
import "./RewardToken.sol";

contract RedemptionKiosk is Ownable {
    IERC20 public immutable ct;       // ChoreToken (ERC20)
    RewardToken public immutable rt;  // RewardToken (ERC20 with KIOSK_ROLE)
    uint256 public ctPerRt;           // CT cost for 1 RT (e.g., 50)

    event Redeemed(address indexed child, uint256 ctSpent, uint256 rtMinted);
    event RateChanged(uint256 oldRate, uint256 newRate);

    constructor(address ct_, address rt_, uint256 ctPerRt_) Ownable(msg.sender) {
        require(ct_ != address(0) && rt_ != address(0), "bad addr");
        require(ctPerRt_ > 0, "rate>0");
        ct = IERC20(ct_);
        rt = RewardToken(rt_);
        ctPerRt = ctPerRt_;
    }

    function setRate(uint256 newRate) external onlyOwner {
        require(newRate > 0, "rate>0");
        emit RateChanged(ctPerRt, newRate);
        ctPerRt = newRate;
    }

    // Child calls after approving CT to this contract
    function redeem(uint256 desiredRt) external {
        require(desiredRt > 0, "rt>0");
        uint256 cost = desiredRt * ctPerRt;

        // pull CT from child into kiosk
        bool ok = ct.transferFrom(msg.sender, address(this), cost * 1e18);
        require(ok, "CT transfer failed");

        // mint RT to child (1 RT has 18 decimals)
        rt.mint(msg.sender, desiredRt * 1e18);

        emit Redeemed(msg.sender, cost * 1e18, desiredRt * 1e18);
    }
}
