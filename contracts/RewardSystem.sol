pragma solidity ^0.8.20;

contract RewardSystem {
    mapping(address => uint256) public points;

    function givePoints(address child, uint256 amount) external {
        points[child] += amount;
    }

    function redeemPoints(address child, uint256 amount) external {
        require(points[child] >= amount, "Not enough points");
        points[child] -= amount;
    }
}
