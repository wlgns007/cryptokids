// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

interface IRewardToken {
    function mint(address to, uint256 value) external;
}

interface IBadgeVC {
    function consume(address user, string calldata badgeId) external returns (bool);
}

contract Kiosk {
    address public owner;
    IRewardToken public rt;
    IBadgeVC public vc;

    mapping(bytes32 => uint256) public bonusByBadge; // badgeId -> RT amount (18 decimals)

    modifier onlyOwner() { require(msg.sender == owner, "not owner"); _; }

    constructor(address rt_, address vc_) {
        owner = msg.sender;
        rt = IRewardToken(rt_);
        vc = IBadgeVC(vc_);
    }

    function _key(string memory badgeId) internal pure returns (bytes32) {
        return keccak256(abi.encodePacked(badgeId));
    }

    function setBonus(string calldata badgeId, uint256 amount) external onlyOwner {
        bonusByBadge[_key(badgeId)] = amount;
    }

    function redeem(string calldata badgeId) external {
        require(vc.consume(msg.sender, badgeId), "consume failed");
        uint256 amount = bonusByBadge[_key(badgeId)];
        require(amount > 0, "badge not configured");
        rt.mint(msg.sender, amount);
    }
}
