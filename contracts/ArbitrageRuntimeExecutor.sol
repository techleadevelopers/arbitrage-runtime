// SPDX-License-Identifier: MIT
pragma solidity ^0.8.24;

import {ArbitrageRuntimeExecutorBase} from "./base/ArbitrageRuntimeExecutorBase.sol";
import {IArbitrageRuntimeExecutor} from "./interfaces/IArbitrageRuntimeExecutor.sol";

/// @title ArbitrageRuntimeExecutor
/// @notice Deployable executor skeleton aligned with the Rust runtime ABI.
/// @dev This contract is intentionally strict: it implements the state machine,
///      authorization, callbacks, and settlement surface, but leaves router-specific
///      swap execution and V2 debt settlement hooks to be completed for the target venue set.
///      In its current form it is a production-shaped base, not a profitable deployable strategy.
contract ArbitrageRuntimeExecutor is ArbitrageRuntimeExecutorBase {
    event RouterIntegrationPending(bytes32 indexed executionId, uint256 indexed stepIndex, address indexed router);
    event V2DebtSettlementPending(bytes32 indexed executionId, address indexed pair);
    event V3DebtSettlementPending(bytes32 indexed executionId, address indexed pool);

    error RouterIntegrationRequired(address router);
    error V2DebtSettlementRequired(address pair);
    error V3DebtSettlementRequired(address pool);

    constructor(address initialOwner) ArbitrageRuntimeExecutorBase(initialOwner) {}

    function _executeV2Step(
        IArbitrageRuntimeExecutor.V2SwapStep memory step,
        uint256 stepIndex,
        V2CallbackContext memory ctx
    ) internal override returns (address tokenIn, address tokenOut, uint256 amountOut) {
        emit RouterIntegrationPending(ctx.executionId, stepIndex, step.router);
        tokenIn = step.path[0];
        tokenOut = step.path[step.path.length - 1];
        amountOut = 0;
        revert RouterIntegrationRequired(step.router);
    }

    function _executeV3Step(
        IArbitrageRuntimeExecutor.V3SwapStep memory step,
        uint256 stepIndex,
        V3CallbackContext memory ctx
    ) internal override returns (uint256 amountOut) {
        emit RouterIntegrationPending(ctx.executionId, stepIndex, step.router);
        amountOut = 0;
        revert RouterIntegrationRequired(step.router);
    }

    function _settleV2PairDebt(
        V2CallbackContext memory ctx,
        uint256,
        uint256
    ) internal override returns (uint256 repaymentAmount) {
        emit V2DebtSettlementPending(ctx.executionId, ctx.pair);
        repaymentAmount = 0;
        revert V2DebtSettlementRequired(ctx.pair);
    }

    function _settleV3PoolDebt(
        V3CallbackContext memory ctx,
        int256,
        int256
    ) internal override returns (uint256 repaymentAmount) {
        emit V3DebtSettlementPending(ctx.executionId, ctx.pool);
        repaymentAmount = 0;
        revert V3DebtSettlementRequired(ctx.pool);
    }
}
