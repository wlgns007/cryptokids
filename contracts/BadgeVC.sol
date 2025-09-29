// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

contract BadgeVC {
    address public owner;
    mapping(address => mapping(bytes32 => bool)) public hasBadge;
    mapping(address => mapping(bytes32 => bool)) public consumed;
    mapping(address => bool) public isRedeemer; // Kiosk addresses

    event Issued(address indexed to, bytes32 indexed id);
    event Consumed(address indexed user, bytes32 indexed id);
    event RedeemerSet(address indexed kiosk, bool enabled);

    modifier onlyOwner()  { require(msg.sender == owner, "not owner"); _; }
    modifier onlyRedeemer(){ require(isRedeemer[msg.sender], "not redeemer"); _; }

    constructor() { owner = msg.sender; }

    function _key(string memory badgeId) internal pure returns (bytes32) {
        return keccak256(abi.encodePacked(badgeId));
    }

    function setRedeemer(address kiosk, bool enabled) external onlyOwner {
        isRedeemer[kiosk] = enabled;
        emit RedeemerSet(kiosk, enabled);
    }

    function issue(address to, string calldata badgeId) external onlyOwner {
        bytes32 k = _key(badgeId);
        hasBadge[to][k] = true;
        emit Issued(to, k);
    }

    // Kiosk calls this to mark a badge as spent (one-time)
    function consume(address user, string calldata badgeId) external onlyRedeemer returns (bool) {
        bytes32 k = _key(badgeId);
        require(hasBadge[user][k], "no badge");
        require(!consumed[user][k], "already used");
        consumed[user][k] = true;
        emit Consumed(user, k);
        return true;
    }
}
