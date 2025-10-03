/* SPDX-License-Identifier: MIT */
pragma solidity ^0.8.20;

interface IRT {
    function transferFrom(address from, address to, uint256 value) external returns (bool);
}

contract ParentsShop {
    address public owner;
    address public treasury;
    IRT public rt;

    struct Item { uint256 price; bool active; } // price in RT wei (18 decimals)
    mapping(uint256 => Item) public items;

    modifier onlyOwner() { require(msg.sender == owner, "not owner"); _; }

    constructor(address rt_, address treasury_) {
        owner = msg.sender;
        rt = IRT(rt_);
        treasury = treasury_;
    }

    function setTreasury(address t) external onlyOwner { treasury = t; }

    function setItem(uint256 id, uint256 price, bool active) external onlyOwner {
        items[id] = Item(price, active);
    }

    function buy(uint256 id, uint256 qty) external {
        Item memory it = items[id];
        require(it.active, "inactive");
        uint256 total = it.price * qty;
        require(rt.transferFrom(msg.sender, treasury, total), "pay fail");
    }
}
