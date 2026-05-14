// SPDX-License-Identifier: MIT
pragma solidity ^0.8.24;

import {
    ArbitrageRuntimeExecutorBase,
    IUniswapV2RouterLike,
    IUniswapV3PoolLike,
    IUniswapV3RouterLike
} from "./base/ArbitrageRuntimeExecutorBase.sol";
import {IArbitrageRuntimeExecutor} from "./interfaces/IArbitrageRuntimeExecutor.sol";

/// @title ArbitrageRuntimeExecutor
/// @notice Concrete executor aligned with the Rust runtime ABI.
/// @dev Assumes ERC20-based routed execution. Native-token wrapping/unwrapping,
///      venue-specific fee-on-transfer handling, and specialized router variants
///      should be layered on top if your target venue set requires them.
contract ArbitrageRuntimeExecutor is ArbitrageRuntimeExecutorBase {
    constructor(address initialOwner) ArbitrageRuntimeExecutorBase(initialOwner) {}

    function _executeV2Step(
        IArbitrageRuntimeExecutor.V2SwapStep memory step,
        uint256 stepIndex,
        V2CallbackContext memory
    ) internal override returns (address tokenIn, address tokenOut, uint256 amountOut) {
        tokenIn = step.path[0];
        tokenOut = step.path[step.path.length - 1];

        uint256 amountIn = _currentAmountIn(step.amountIn, tokenIn);
        if (amountIn == 0) revert ZeroAmount();

        _forceApprove(tokenIn, step.router, amountIn);
        uint256 beforeOut = _balanceOf(tokenOut);
        uint256[] memory amounts = IUniswapV2RouterLike(step.router).swapExactTokensForTokens(
            amountIn,
            step.minOut,
            step.path,
            address(this),
            block.timestamp
        );
        if (amounts.length == 0) revert StepExecutionFailed(stepIndex);
        amountOut = amounts[amounts.length - 1];

        uint256 afterOut = _balanceOf(tokenOut);
        if (afterOut < beforeOut) revert StepExecutionFailed(stepIndex);
        uint256 balanceDiff = afterOut - beforeOut;
        if (balanceDiff > amountOut) {
            amountOut = balanceDiff;
        }
        if (amountOut < step.minOut) revert StepExecutionFailed(stepIndex);
    }

    function _executeV3Step(
        IArbitrageRuntimeExecutor.V3SwapStep memory step,
        uint256 stepIndex,
        V3CallbackContext memory
    ) internal override returns (uint256 amountOut) {
        address tokenIn = _v3PathTokenIn(step.path);
        address tokenOut = _v3PathTokenOut(step.path);
        uint256 amountIn = _currentAmountIn(step.amountIn, tokenIn);
        if (amountIn == 0) revert ZeroAmount();

        _forceApprove(tokenIn, step.router, amountIn);
        uint256 beforeOut = _balanceOf(tokenOut);
        amountOut = IUniswapV3RouterLike(step.router).exactInput(
            IUniswapV3RouterLike.ExactInputParams({
                path: step.path,
                recipient: address(this),
                deadline: block.timestamp,
                amountIn: amountIn,
                amountOutMinimum: step.minOut
            })
        );

        uint256 afterOut = _balanceOf(tokenOut);
        if (afterOut < beforeOut) revert StepExecutionFailed(stepIndex);
        uint256 balanceDiff = afterOut - beforeOut;
        if (balanceDiff > amountOut) {
            amountOut = balanceDiff;
        }
        if (amountOut < step.minOut) revert StepExecutionFailed(stepIndex);
    }

    function _settleV2PairDebt(
        V2CallbackContext memory ctx,
        uint256 amount0,
        uint256 amount1
    ) internal override returns (uint256 repaymentAmount) {
        uint256 borrowedAmount = amount0 > 0 ? amount0 : amount1;
        if (borrowedAmount == 0) {
            borrowedAmount = ctx.borrowAmount;
        }
        repaymentAmount = _v2RepaymentAmount(borrowedAmount);
        _transferToken(ctx.borrowToken, ctx.pair, repaymentAmount);
    }

    function _settleV3PoolDebt(
        V3CallbackContext memory ctx,
        int256 amount0Delta,
        int256 amount1Delta
    ) internal override returns (uint256 repaymentAmount) {
        address token0 = IUniswapV3PoolLike(ctx.pool).token0();
        address token1 = IUniswapV3PoolLike(ctx.pool).token1();

        if (amount0Delta > 0) {
            uint256 owed0 = uint256(amount0Delta);
            _transferToken(token0, ctx.pool, owed0);
            repaymentAmount += owed0;
        }
        if (amount1Delta > 0) {
            uint256 owed1 = uint256(amount1Delta);
            _transferToken(token1, ctx.pool, owed1);
            repaymentAmount += owed1;
        }
    }
}
