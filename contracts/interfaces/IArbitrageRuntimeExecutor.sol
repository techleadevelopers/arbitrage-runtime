// SPDX-License-Identifier: MIT
pragma solidity ^0.8.24;

/// @title IArbitrageRuntimeExecutor
/// @notice Canonical on-chain executor ABI expected by the Rust runtime.
/// @dev This interface describes the live execution surface for deterministic
///      fee extraction around AMM swaps across Uniswap V2-style and Uniswap V3-style paths.
///      The Rust runtime assumes an executor implementing these entrypoints can:
///      - initiate V2 flashswap-backed execution
///      - initiate V3 pool-backed execution
///      - process routed swap steps
///      - validate profitability before releasing control
///      - repay the borrowed side atomically
interface IArbitrageRuntimeExecutor {
    /// @notice Single routed swap step for V2-style router execution.
    /// @param router Router used for the swap step.
    /// @param path Swap path expressed as token addresses.
    /// @param amountIn Requested input amount for the step.
    /// @param minOut Minimum acceptable output to preserve execution constraints.
    struct V2SwapStep {
        address router;
        address[] path;
        uint256 amountIn;
        uint256 minOut;
    }

    /// @notice Single routed swap step for V3-style router execution.
    /// @param router Router used for the swap step.
    /// @param path Encoded Uniswap V3 path bytes.
    /// @param amountIn Requested input amount for the step.
    /// @param minOut Minimum acceptable output to preserve execution constraints.
    struct V3SwapStep {
        address router;
        bytes path;
        uint256 amountIn;
        uint256 minOut;
    }

    /// @notice Top-level V2 flashswap execution request.
    /// @param pair Borrow source pair.
    /// @param borrowToken Token borrowed from the pair.
    /// @param borrowAmount Amount borrowed from the pair.
    /// @param minProfit Minimum required net profit before successful completion.
    /// @param profitToken Token in which profitability is evaluated.
    /// @param profitRecipient Address that receives realized profit.
    /// @param steps Ordered execution steps used after borrowing liquidity.
    struct V2ExecutionRequest {
        address pair;
        address borrowToken;
        uint256 borrowAmount;
        uint256 minProfit;
        address profitToken;
        address profitRecipient;
        V2SwapStep[] steps;
    }

    /// @notice Top-level V3 pool execution request.
    /// @param pool Borrow source pool.
    /// @param borrowToken Token borrowed from the pool swap path.
    /// @param borrowAmount Amount borrowed from the pool path.
    /// @param feeTier Fee tier associated with the V3 pool.
    /// @param minProfit Minimum required net profit before successful completion.
    /// @param profitToken Token in which profitability is evaluated.
    /// @param profitRecipient Address that receives realized profit.
    /// @param steps Ordered execution steps used after borrowing liquidity.
    struct V3ExecutionRequest {
        address pool;
        address borrowToken;
        uint256 borrowAmount;
        uint24 feeTier;
        uint256 minProfit;
        address profitToken;
        address profitRecipient;
        V3SwapStep[] steps;
    }

    /// @notice Emitted when a V2 execution path is initiated.
    event V2ExecutionStarted(
        address indexed initiator,
        address indexed pair,
        address indexed borrowToken,
        uint256 borrowAmount,
        uint256 minProfit,
        address profitToken
    );

    /// @notice Emitted when a V3 execution path is initiated.
    event V3ExecutionStarted(
        address indexed initiator,
        address indexed pool,
        address indexed borrowToken,
        uint24 feeTier,
        uint256 borrowAmount,
        uint256 minProfit,
        address profitToken
    );

    /// @notice Emitted per execution step for V2-style flow.
    event V2StepExecuted(
        uint256 indexed stepIndex,
        address indexed router,
        address tokenIn,
        address tokenOut,
        uint256 amountIn,
        uint256 amountOut
    );

    /// @notice Emitted per execution step for V3-style flow.
    event V3StepExecuted(
        uint256 indexed stepIndex,
        address indexed router,
        bytes path,
        uint256 amountIn,
        uint256 amountOut
    );

    /// @notice Emitted when an execution fully settles.
    event ExecutionSettled(
        bytes32 indexed executionId,
        address indexed profitToken,
        uint256 grossProfit,
        uint256 netProfit,
        uint256 repaymentAmount
    );

    error ZeroAddress();
    error ZeroAmount();
    error InvalidPath();
    error InvalidStep();
    error InvalidCaller();
    error UnsupportedPair();
    error UnsupportedPool();
    error UnsupportedBorrowToken();
    error UnsupportedFeeTier();
    error StepExecutionFailed(uint256 stepIndex);
    error RepaymentFailed();
    error ProfitBelowMinimum(uint256 realizedProfit, uint256 minimumProfit);

    /// @notice Initiates a Uniswap V2-style flashswap-backed execution.
    /// @param pair Borrow source pair.
    /// @param borrowToken Token borrowed from the pair.
    /// @param borrowAmount Amount borrowed from the pair.
    /// @param minProfit Minimum acceptable net profit in `profitToken` units.
    /// @param profitToken Token used to evaluate profitability.
    /// @param profitRecipient Address that receives realized profit.
    /// @param steps Ordered swap steps executed after the borrow.
    function startV2FlashSwap(
        address pair,
        address borrowToken,
        uint256 borrowAmount,
        uint256 minProfit,
        address profitToken,
        address profitRecipient,
        V2SwapStep[] calldata steps
    ) external;

    /// @notice Initiates a Uniswap V3-style pool-backed execution.
    /// @param pool Borrow source pool.
    /// @param borrowToken Token borrowed from the V3 pool path.
    /// @param borrowAmount Amount borrowed from the pool path.
    /// @param feeTier Pool fee tier.
    /// @param minProfit Minimum acceptable net profit in `profitToken` units.
    /// @param profitToken Token used to evaluate profitability.
    /// @param profitRecipient Address that receives realized profit.
    /// @param steps Ordered swap steps executed after the borrow.
    function startV3FlashSwap(
        address pool,
        address borrowToken,
        uint256 borrowAmount,
        uint24 feeTier,
        uint256 minProfit,
        address profitToken,
        address profitRecipient,
        V3SwapStep[] calldata steps
    ) external;

    /// @notice Required callback surface for Uniswap V2-style pair callbacks.
    /// @dev An implementation is expected to validate caller authenticity and
    ///      execute the step plan encoded by `data`.
    function uniswapV2Call(
        address sender,
        uint256 amount0,
        uint256 amount1,
        bytes calldata data
    ) external;

    /// @notice Required callback surface for Uniswap V3-style pool callbacks.
    /// @dev An implementation is expected to validate caller authenticity and
    ///      settle any positive deltas before returning control to the pool.
    function uniswapV3SwapCallback(
        int256 amount0Delta,
        int256 amount1Delta,
        bytes calldata data
    ) external;
}
