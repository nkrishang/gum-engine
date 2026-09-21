// SPDX-License-Identifier: MIT
pragma solidity 0.8.30;

/// Ground-truth oracle for gum-bench. Every bench job calls `hit(id)` (or `burn`) with a unique
/// random id, so the chain itself records whether a job executed zero, one or (never) two times.
///
/// The creation bytecode is committed in `BenchTarget.hex`; regenerate it with
/// `crates/gum-bench/scripts/regen-bytecode.sh` after editing this file.
contract BenchTarget {
    mapping(bytes32 => uint256) public hits;
    uint256 public total;

    event Hit(bytes32 indexed id, address indexed sender);

    function hit(bytes32 id) external {
        _record(id);
    }

    /// Same as `hit`, plus `iterations` SSTORE-free keccak rounds to burn gas.
    function burn(bytes32 id, uint256 iterations) external {
        _record(id);
        bytes32 acc = id;
        for (uint256 i = 0; i < iterations; i++) {
            acc = keccak256(abi.encodePacked(acc, i));
        }
    }

    /// Always reverts. Callers may append arbitrary trailing calldata (gum-bench appends the job's
    /// unique id) — Solidity ignores it, and it makes each reverting transaction attributable on-chain.
    function fail() external pure {
        revert("bench: fail");
    }

    function _record(bytes32 id) private {
        require(hits[id] == 0, "dup");
        hits[id] = 1;
        total += 1;
        emit Hit(id, msg.sender);
    }
}
