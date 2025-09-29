// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

contract RewardToken {
    string public name = "RewardToken";
    string public symbol = "RT";
    uint8  public constant decimals = 18;

    uint256 public totalSupply;
    address public owner;

    mapping(address => uint256) public balanceOf;
    mapping(address => mapping(address => uint256)) public allowance;
    mapping(address => bool) public isMinter;

    event Transfer(address indexed from, address indexed to, uint256 value);
    event Approval(address indexed owner_, address indexed spender, uint256 value);
    event MinterSet(address indexed minter, bool enabled);

    modifier onlyOwner() { require(msg.sender == owner, "not owner"); _; }

    constructor() { owner = msg.sender; }

    function setMinter(address minter, bool enabled) external onlyOwner {
        isMinter[minter] = enabled;
        emit MinterSet(minter, enabled);
    }

    function _transfer(address from, address to, uint256 value) internal {
        require(to != address(0), "zero addr");
        uint256 bal = balanceOf[from];
        require(bal >= value, "insufficient");
        unchecked { balanceOf[from] = bal - value; }
        balanceOf[to] += value;
        emit Transfer(from, to, value);
    }

    function transfer(address to, uint256 value) external returns (bool) {
        _transfer(msg.sender, to, value);
        return true;
    }

    function approve(address spender, uint256 value) external returns (bool) {
        allowance[msg.sender][spender] = value;
        emit Approval(msg.sender, spender, value);
        return true;
    }

    function transferFrom(address from, address to, uint256 value) external returns (bool) {
        uint256 allowed = allowance[from][msg.sender];
        require(allowed >= value, "allowance");
        if (allowed != type(uint256).max) {
            unchecked { allowance[from][msg.sender] = allowed - value; }
        }
        _transfer(from, to, value);
        return true;
    }

    function mint(address to, uint256 value) external {
        require(msg.sender == owner || isMinter[msg.sender], "no mint role");
        totalSupply += value;
        balanceOf[to] += value;
        emit Transfer(address(0), to, value);
    }
}
